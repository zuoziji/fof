# -*- coding: utf-8 -*-
"""
:copyright: (c) 2016 by Huang Dong 
:mail: bjdhuang@cn.ibm.com.
:license: Apache 2.0, see LICENSE for more details.
"""


from flask_admin.contrib.sqla import ModelView
from ..models import FoFModel,get_all_fof
from logging import getLogger
from ..extensions import cache
from wtforms import PasswordField
logger = getLogger()

class CustomView(ModelView):
    pass
    # list_template = 'manager/list.html'
    # create_template = 'manager/create.html'
    # edit_template = 'manager/edit.html'

class FofAdmin(CustomView):
    column_display_pk = True
    form_choices = {'strategy_type': [
                    ('股票多头策略','股票多头策略'),
                    ('股票多空策略', '股票多空策略'),
                    ('事件驱动策略', '事件驱动策略'),
                    ('其他股票策略', '其他股票策略'),
                    ('阿尔法策略', '阿尔法策略'),
                    ('债券策略','债券策略'),
                    ('货币市场策略', '货币市场策略'),
                    ('管理期货策略', '管理期货策略'),
                    ('套利策略', '套利策略'),
                    ('宏观策略', '宏观策略'),
                    ('组合基金策略','组合基金策略'),
                    ('现金管理', '现金管理'),
                    ],
                    'rank':[
                        ('0',"未评级"),
                        ('1',"不关注"),
                        ('2',"观察"),
                        ('3',"备选"),
                        ('4',"核心池")
                    ]}
    form_columns = ['wind_code','sec_name','strategy_type','fund_setupdate','fund_maturitydate','fund_mgrcomp','fund_status','alias',
                    'fund_existingyear','fund_ptmyear','fund_type','fund_fundmanager','nav_acc_latest','nav_acc_mdd','sharpe',
                    'nav_date_latest','annual_return','scale_tot','scale_a','scale_b','priority_asset','inferior_asset',
                    'priority_interest_rate','rank','file','fh_inv_manager','fh_prod_manager','fh_channel_manager','nav_maintain_mode']

    column_labels = dict(
        wind_code='基金代码',sec_name='基金名称',strategy_type='策略名称',fund_setupdate='成立时间',fund_maturitydate='终止日',
        fund_mgrcomp='基金经理',fund_status='基金状态',alias='别名',
                    fund_existingyear='存在年限',fund_ptmyear='存续年限',fund_type='基金类型',fund_fundmanager='基金管理人员',
        nav_acc_latest='最新净值',nav_acc_mdd="最大回撤比",sharpe='夏普比',
                    nav_date_latest="最新净值日期",annual_return="年化收益率",scale_tot="总规模",scale_a="A类份额规模",scale_b="B类份额规模",
        priority_asset="优先级资产规模",inferior_asset="劣后级资产规模",fh_inv_manager="投资负责人",fh_prod_manager="产品负责人",fh_channel_manager="渠道负责人",
                    priority_interest_rate="优先级年化收益率",rank="基金评级信息",file="文件",nav_maintain_mode='净值模式')

    column_searchable_list = ('wind_code','sec_name')
    column_list = ('wind_code','sec_name','strategy_type','fund_setupdate','fund_maturitydate','fund_mgrcomp','fund_status')
    export_max_rows = 10

class PctAdmin(ModelView):
    column_display_pk = True
    column_labels = dict(
        invest_scale='投资规模',
        date_adj = '调整日期',
        wind_code_s = "子基金",
        fund_info ="母基金"
    )
    column_list = ('wind_code_s', 'fund_info', 'date_adj', 'invest_scale')

    form_ajax_refs = {
        'fund_info': {
            'fields': (FoFModel.wind_code, FoFModel.sec_name, FoFModel.alias)
        }

    }

class UserAdmin(ModelView):
    column_display_pk = True
    column_labels = dict(
        username = "用户名",
        email="邮箱",
        password_hash = "密码",
        role = "角色",
        is_admin= "管理员",
        is_staff="复华",
        update_nav="净值修改",
        is_report="研究员",
        confirmed='已激活'
    )
    column_list = ('username', 'email','password_hash','is_admin','is_staff','update_nav','is_report')
    column_formatters = dict(
        password_hash=lambda v, c, m, p: '*****' + m.password_hash[-6:],
    )
    form_excluded_columns = ('password_hash')
    form_extra_fields = {
        'password2': PasswordField('密码哈希')
    }

    def on_model_change(self, form, User, is_created):
        if len(form.password2.data) > 0 :
            User.password_hash = User.set_password(form.password2.data)
        fof_list = get_all_fof(User)
        if fof_list is None:
            logger.warning("用户没有可管理的基金,删除缓存")
            cache.delete(str(User.id))
        else:
            logger.info("用户{}的缓存已更新".format(User.username))
            cache.set(str(User.id), fof_list)



class StgAdmin(ModelView):
    form_choices = {'stg_code': [
        ('股票多头策略', '股票多头策略'),
        ('股票多空策略', '股票多空策略'),
        ('事件驱动策略', '事件驱动策略'),
        ('其他股票策略', '其他股票策略'),
        ('阿尔法策略', '阿尔法策略'),
        ('债券策略', '债券策略'),
        ('货币市场策略', '货币市场策略'),
        ('管理期货策略', '管理期货策略'),
        ('套利策略', '套利策略'),
        ('宏观策略', '宏观策略'),
        ('现金管理', '现金管理'),
    ]}
    column_labels = dict(
        fund_info='基金名称',
        stg_code='策略类型',
        trade_date='调整日期',
        stg_pct='策略比例'
    )
    column_list = ('wind_code','stg_code','trade_date','stg_pct')
    form_ajax_refs = {
        'fund_info': {
            'fields': (FoFModel.wind_code, FoFModel.sec_name, FoFModel.alias)
        }
    }

class RoleAdmin(ModelView):
    column_display_pk = True
    column_labels = dict(
        name ='角色名称',
        permissions = '权限',
        file_type ='文件类型',
        fof = '母基金',
        user = "用户名"
    )
    column_list = ('name','fof', 'user','permissions','file_type')

    form_ajax_refs = {
        'fof': {
            'fields': (FoFModel.wind_code,FoFModel.sec_name,FoFModel.alias)
        }
    }

    def after_model_change(self, form, model, is_created):
        user = model.user
        if len(user) > 0 :
            for i in user:
                fof_list = get_all_fof(i)
                if fof_list is None:
                    logger.warning("用户没有可管理的基金,删除缓存")
                    cache.delete(str(i.id))
                else:
                    logger.info("用户{}的缓存已更新".format(i.username))
                    cache.set(str(i.id),fof_list)

class PerAdmin(ModelView):
    column_display_pk = True
    column_labels = dict(
        name='权限名称',
        action = "函数名称",
        roles="角色"
    )
    column_list = ('name', 'action', 'roles')


class FileTypeAdmin(ModelView):
    column_display_pk = True
    column_labels = dict(
        file='文件',
        type_name = "类型",
        role="角色"
    )
    column_list = ('file', 'type_name', 'roles')
    form_columns = ["type_name",'file']



class FileAdmin(ModelView):
    column_display_pk = True
    column_labels = dict(
        wind_code='基金',
        show_name='文件名称',
        type_name='文件类型',
        file_path='文件路径',
        upload_datetime="上传时间",
        fund_info="母基金"
    )
    column_list = ('fund_info', 'show_name', 'type_name','file_path','upload_datetime')
    form_ajax_refs = {
        'fund_info': {
            'fields': (FoFModel.wind_code, FoFModel.sec_name, FoFModel.alias)
        }
    }


class AccAdmin(ModelView):
    column_display_pk = True
    form_columns =["wind_code","nav_date","nav","nav_acc","source_mark","nav_tot"]
    column_searchable_list = ('wind_code',)


class SecAdmin(ModelView):
    column_display_pk = True

class EventAdmin(ModelView):
    form_choices = {'event_type': [
        ('投资事项', '投资事项'),
        ('投后事项', '投后事项'),
        ('法务事项', '法务事项'),
        ('其他事项', '其他事项'),
    ]}
    column_labels = dict(
        fund_info='基金名称',
        event_date='到期时间',
        event_type='事件类型',
        remind_date='提醒日期',
        handle_status='提醒状态',
        notation="消息正文",
        wind_code='基金名称',
        create_user="用户"

    )
    column_list = ('wind_code','event_date','event_type','remind_date','handle_status','create_user')
    form_ajax_refs = {
        'fund_info': {
            'fields': (FoFModel.wind_code, FoFModel.sec_name, FoFModel.alias)
        }
    }

class ChildMapping(ModelView):
    column_display_pk = True
    form_columns = ['wind_code_s','wind_code','sec_name_s','date_start','date_end','warning_line','winding_line']
    column_labels = dict(
        wind_code_s = '批次代码',
        wind_code = '子基金代码',
        sec_name_s = '批次名称',
        date_start = '开始时间',
        date_end = '结束时间',
        warning_line = '预警线',
        winding_line = '清盘线'
    )

class Invest_corp_admin(ModelView):
    column_display_pk = True
    form_columns = ["name", "alias", "review_status"]
    column_searchable_list = ('name',)

class Invest_corp_file_admin(ModelView):
    column_display_pk =  True
    form_columns = ["file_id",'mgrcomp_id','file_type','upload_user_id','upload_datetime','file_name']