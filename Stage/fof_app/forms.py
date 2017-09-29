# -*- coding: utf-8 -*-
"""
:copyright: (c) 2016 by Huang Dong 
:mail: bjdhuang@cn.ibm.com.
:license: Apache 2.0, see LICENSE for more details.
"""

from flask_wtf import FlaskForm
from wtforms import StringField,PasswordField,BooleanField,DateField,FloatField,SelectField,SubmitField
from wtforms.validators import DataRequired,Length,EqualTo,Email,ValidationError
from .models import UserModel
import logging
logger = logging.getLogger(__name__)



class FOFSummary(FlaskForm):
    wind_code = StringField('基金代码',render_kw={'disabled':''})
    sec_name = StringField("基金名称",render_kw={'disabled':''} )
    strategy_type = SelectField("策略类型",choices=[
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
                    ("组合基金策略","组合基金策略"),
                    ("现金", "现金"),
                    ])
    fund_setupdate = DateField("成立时间")
    fund_maturitydate = DateField("终止时间")
    fund_mgrcomp = StringField("基金经理")
    fund_existingyear = FloatField("存在年限")
    fund_ptmyear = FloatField("存续年限")
    fund_type = StringField("基金类型")
    fund_fundmanager = StringField("基金管理人员")
    fund_status = BooleanField("基金状态")
    nav_maintain_mode = BooleanField("净值维护方式")
    alias = StringField("别名")
    scale_tot = FloatField("总规模")
    scale_a = FloatField("A类份额规模")
    scale_b = FloatField("B类份额规模")
    priority_asset = FloatField("优先级资产规模")
    inferior_asset = FloatField("劣后级资产规模")
    priority_interest_rate = FloatField("优先级年化收益率")
    rank = SelectField("评级信息",coerce=int,choices=[(0,"未评级"),
                        (1,"不关注"),
                        (2,"观察"),
                        (3,"备选"),
                        (4,"核心池")])
    annual_return = FloatField("化收益率")
    nav_acc_mdd = FloatField("最大回撤比")
    sharpe = FloatField("夏普比")
    nav_acc_latest = FloatField("最新净值")
    nav_date_latest = DateField("最新净值日期")
    fh_inv_manager = StringField("投资管理人")
    fh_prod_manager = StringField("产品负责人")
    fh_channel_manager = StringField("渠道负责人")




    def validate(self):
        check_validate = super(FOFSummary,self).validate()
        logging.info("{}".format(self.errors))
        if not check_validate:
            return True
        return  True


class ChildFofForm(FlaskForm):
    wind_code_p = SelectField("母基金代码")
    wind_code = SelectField("子基金代码")
    date_adj = DateField("调仓日期",[DataRequired()])
    invest_scale = FloatField("投资规模")
    nac_acc_start = FloatField("起始净值",[DataRequired()])
    warning_line = FloatField("预警线",[DataRequired()])
    winding_line = FloatField("清盘线",[DataRequired()])


class LoginForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(), Length(1, 64),
                                             Email()],render_kw={"placeholder": "邮箱"})
    password = PasswordField('Password', validators=[DataRequired()],render_kw={"placeholder": "密码"})
    remember_me = BooleanField('记住我')
    submit = SubmitField('登录')

class RegisterForm(FlaskForm):
    username = StringField('username',[DataRequired(),Length(max=255)],render_kw={"placeholder": "用户名"})
    password = PasswordField('password',[PasswordField,Length(min=6)],render_kw={"placeholder": "密码"})
    confirm = PasswordField('confirm password',[DataRequired(),EqualTo('password')],render_kw={"placeholder": "确认密码"})
    verification_code = StringField('code', validators=[DataRequired()], render_kw={"placeholder": "验证码"})

    def validate(self):
        check_validate = super(RegisterForm,self).validate()
        if not check_validate:
            return False
        user = UserModel.query.filter_by(username=self.username.data).first()
        if user:
            self.username.errors.append("用户名已存在")
            return False
        return True



class ChangePasswordForm(FlaskForm):
    old_password = PasswordField('Old password', validators=[DataRequired()])
    password = PasswordField('New password', validators=[
        DataRequired(), EqualTo('password2', message='Passwords must match')])
    password2 = PasswordField('Confirm new password', validators=[DataRequired()])
    submit = SubmitField('Update Password')


class PasswordResetRequestForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(), Length(1, 64),
                                             Email()],render_kw={"placeholder": "请输入你的邮箱"})
    submit = SubmitField('Reset Password')


class PasswordResetForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(), Length(1, 64),
                                             Email()],render_kw={"placeholder": "请输入你的邮箱"})
    password = PasswordField('New Password', validators=[
        DataRequired(), EqualTo('password2', message='Passwords must match')],render_kw={"placeholder": "请输入你的密码"})
    password2 = PasswordField('Confirm password', validators=[DataRequired()],render_kw={"placeholder": "请再次输入你的密码"})
    submit = SubmitField('Reset Password')

    def validate_email(self, field):
        if UserModel.query.filter_by(email=field.data).first() is None:
            raise ValidationError('未知的邮箱地址')