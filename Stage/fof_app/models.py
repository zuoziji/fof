# -*- coding: utf-8 -*-
"""
:copyright: (c) 2016 by Huang Dong 
:mail: bjdhuang@cn.ibm.com.
:license: Apache 2.0, see LICENSE for more details.
"""

from sqlalchemy import event, and_
from flask_login import AnonymousUserMixin, current_user
from datetime import datetime
from itsdangerous import TimedJSONWebSignatureSerializer as Serializer
from werkzeug.security import generate_password_hash, check_password_hash
from flask import current_app
import logging
from .tasks import send_email
from sqlalchemy.types import TypeDecorator, String
from cryptography.fernet import Fernet
from flask_sqlalchemy  import  SQLAlchemy


key = "lNXHXIz61VOA6Q1Zc1v5K-udwN1dEfHK8d8DBXA3-MQ="
logger = logging.getLogger()



db = SQLAlchemy()

user_role = db.Table('user_role',  # 用户角色关联表
                     db.Column('user_id', db.Integer, db.ForeignKey('user.id')),
                     db.Column('role_id', db.Integer, db.ForeignKey('role.id')),
                     )

role_permission = db.Table('role_permission',  # 角色权限关联表
                           db.Column('permission_id', db.Integer, db.ForeignKey('permission.id')),
                           db.Column('role_id', db.Integer, db.ForeignKey('role.id')),

                           )

role_fof = db.Table('role_fof',  # 用户菜单关联表
                    db.Column('role_id', db.Integer, db.ForeignKey('role.id')),
                    db.Column('fof_id', db.String(20), db.ForeignKey('fund_info.wind_code')),
                    )

role_type = db.Table('role_type',  # 用户菜单关联表
                     db.Column('role_id', db.Integer, db.ForeignKey('role.id')),
                     db.Column('type_id', db.String(40), db.ForeignKey('file_type.type_name')),
                     )


class EncryptedData(TypeDecorator):
    impl = String

    def __init__(self):
        super().__init__()
        self.cipher = Fernet(key)
    def process_bind_param(self, value, dialect):
        if value is not None:
            value  = self.cipher.encrypt(bytes(value, encoding='utf-8'))
        return value


    def process_result_value(self, value, dialect):
        if value is not None:
            value = self.cipher.decrypt(bytes(value, encoding='utf-8'))
            value = value.decode('utf-8')
        return value


class MyReal(db.REAL):
    scale = 4


class UserModel(db.Model):
    __tablename__ = 'user'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(255))
    email = db.Column(db.String(80))
    password_hash = db.Column(db.String(255))
    is_admin = db.Column(db.Boolean)
    is_staff = db.Column(db.Boolean)
    is_report = db.Column(db.Boolean)
    is_core = db.Column(db.Boolean)
    update_nav = db.Column(db.Boolean)
    confirmed = db.Column(db.Boolean, default=False)
    last_seen = db.Column(db.DateTime(), default=datetime.utcnow)


    # def __init__(self, username, password):
    #     self.username = username
    #     self.password = self.set_password(password)


    @property
    def permissions(self):
        permissions = PermissionModel.query.join(role_permission).join(RoleModel).join(user_role).join(UserModel). \
            filter(
            UserModel.id == self.id
        )
        return permissions

    @property
    def fofs(self):
        fofs = FoFModel.query.join(role_fof).join(RoleModel).join(user_role).join(UserModel). \
            filter(
            UserModel.id == self.id
        ).all()
        return fofs

    @property
    def files(self):
        files = FileType.query.join(role_type).join(RoleModel).join(user_role).join(UserModel). \
            filter(
            UserModel.id == self.id
        ).all()
        return files

    def check(self, action):
        permission = self.permissions.filter(PermissionModel.action == action).first()
        return bool(permission)

    def __repr__(self):
        return self.username

    def set_password(self, password):
        return generate_password_hash(password)

    def verify_password(self, password):
        logger.info("开始校验密码")
        return check_password_hash(self.password_hash, password)

    def is_authenticated(self):
        if isinstance(self, AnonymousUserMixin):
            return False
        else:
            return True

    def is_anonymous(self):
        if isinstance(self, AnonymousUserMixin):
            return True
        else:
            return False

    def is_active(self):
        return True

    def get_id(self):
        return self.id

    @property
    def password(self):
        raise AttributeError('password is not a readable attribute')

    @password.setter
    def password(self, password):
        self.password_hash = generate_password_hash(password)

    def generate_confirmation_token(self, expiration=3600):
        s = Serializer(current_app.config['SECRET_KEY'], expiration)
        return s.dumps({"confirm": self.id})

    def confirm(self, token):
        s = Serializer(current_app.config['SECRET_KEY'])
        logger.info("start check {} token id".format(self.username))
        try:
            data = s.loads(token)
        except:
            return False
        if data.get('confirm') != self.id:
            return False
        self.confirmed = True
        db.session.add(self)
        db.session.commit()
        return True

    def generate_reset_token(self, expiration=3600):
        s = Serializer(current_app.config['SECRET_KEY'], expiration)
        return s.dumps({'reset': self.id})

    def reset_password(self, token, new_password):
        s = Serializer(current_app.config['SECRET_KEY'])
        try:
            data = s.loads(token)
        except:
            return False
        if data.get('reset') != self.id:
            return False
        self.password = new_password
        db.session.add(self)
        db.session.commit()
        return True

    def ping(self):
        self.last_seen = datetime.utcnow()
        db.session.add(self)
        db.session.commit()

    def to_json(self):
        d = {}
        for k,v in self.__dict__.items():
            print(k,v)
            if v is None:
                d[k] = ''
            else:
                d[k] = v
        return d


    def as_dict(self):
        return {c.name: getattr(self, c.name," ") for c in self.__table__.columns}




class Anonymous(AnonymousUserMixin):
    def __init__(self):
        self.username = 'Guest'


class RoleModel(db.Model):
    __tablename__ = 'role'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(20))
    permissions = db.relationship(
        'PermissionModel', secondary='role_permission', backref='roles'
    )
    file_type = db.relationship(
        'FileType',
        secondary=role_type,
        backref=db.backref('role', lazy='dynamic'))

    fof = db.relationship(
        'FoFModel',
        secondary=role_fof,
        backref=db.backref('role', lazy='dynamic'))

    user = db.relationship(
        'UserModel',
        secondary=user_role,
        backref=db.backref(
            'role',
            lazy='dynamic'
        )
    )

    def __repr__(self):
        return self.name


class PermissionModel(db.Model):
    __tablename__ = 'permission'
    id = db.Column(db.Integer(), primary_key=True)
    name = db.Column(db.String(50))
    action = db.Column(db.String(250), unique=True)

    def __repr__(self):
        return self.name


class FoFModel(db.Model):
    __tablename__ = 'fund_info'
    __searchable__ = ['wind_code', 'sec_name']
    wind_code = db.Column(db.String(20), primary_key=True)
    sec_name = db.Column(db.String(100))
    strategy_type = db.Column(db.String(50))
    fund_setupdate = db.Column(db.Date)
    fund_maturitydate = db.Column(db.Date)
    fund_mgrcomp = db.Column(db.String(200))
    fund_existingyear = db.Column(MyReal)
    fund_ptmyear = db.Column(MyReal)
    fund_type = db.Column(db.String(50))
    fund_fundmanager = db.Column(db.String(200))
    fund_status = db.Column(db.Boolean)
    alias = db.Column(db.String(50))
    scale_tot = db.Column(MyReal)
    scale_a = db.Column(MyReal)
    scale_b = db.Column(MyReal)
    priority_asset = db.Column(MyReal)
    inferior_asset = db.Column(MyReal)
    priority_interest_rate = db.Column(MyReal)
    source_mark = db.Column(db.Integer, default=1)
    rank = db.Column(db.Integer)
    annual_return = db.Column(MyReal)
    nav_acc_mdd = db.Column(MyReal)
    sharpe = db.Column(MyReal)
    nav_acc_latest = db.Column(MyReal)
    nav_date_latest = db.Column(db.Date)
    fh_inv_manager = db.Column(db.String(20))
    fh_prod_manager = db.Column(db.String(20))
    fh_channel_manager = db.Column(db.String(20))
    mgrcomp_id = db.Column(db.INT)
    nav_maintain_mode = db.Column(db.Boolean)
    file = db.relationship('FundFile', backref='fund_info', lazy='dynamic')
    fund_pct = db.relationship('FOF_FUND_PCT', backref='fund_info', lazy='dynamic')
    fund_stg = db.relationship('FUND_STG_PCT', backref='fund_info', lazy='dynamic')
    fund_event = db.relationship('FUND_EVENT', backref='fund_info', lazy='dynamic')
    core_info = db.relationship('Fund_Core_Info', backref='fund_info', lazy='dynamic')
    def __repr__(self):
        return self.wind_code

    def __str__(self):
        return self.sec_name


    def to_json(self):
        d = {}
        for k,v in self.__dict__.items():
            if v is None:
                d[k] = ''
            elif isinstance(v,float):
                if len(str(v).split('.')[1]) > 2:
                    d[k] = "{0:.3f}".format(v)
            else:
                d[k] = v
        return d



    def as_dict(self):
        return {c.name: getattr(self, c.name," ") for c in self.__table__.columns}



class Fund_Core_Info(db.Model):
    __tablename__ = 'fund_core_info'
    id = db.Column(db.Integer, primary_key=True)
    wind_code = db.Column(db.String(20), db.ForeignKey('fund_info.wind_code'))
    product_contact_name = db.Column(EncryptedData)
    product_contact_phone = db.Column(EncryptedData)
    product_contact_email = db.Column(EncryptedData)
    fund_manager_name = db.Column(EncryptedData)
    fund_manager_phone = db.Column(EncryptedData)
    fund_manager_email = db.Column(EncryptedData)
    other_contact_name = db.Column(EncryptedData)
    other_contact_phone = db.Column(EncryptedData)
    other_contact_email = db.Column(EncryptedData)

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def __repr__(self):
        return self.wind_code


class FileType(db.Model):
    __tablename__ = 'file_type'
    type_name = db.Column(db.String(40), primary_key=True)
    file = db.relationship('FundFile', backref='file_type', lazy='dynamic')

    def __repr__(self):
        return self.type_name


class FundFile(db.Model):
    __tablename__ = 'fund_file'
    id = db.Column(db.Integer, primary_key=True)
    wind_code = db.Column(db.String(20), db.ForeignKey('fund_info.wind_code'))
    show_name = db.Column(db.String(500))
    type_name = db.Column(db.String(40), db.ForeignKey('file_type.type_name'))
    upload_datetime = db.Column(db.DateTime)
    file_content = db.Column(db.BLOB)

    def __repr__(self):
        return self.show_name


class FOF_FUND_PCT(db.Model):
    __tablename__ = 'fof_fund_pct'
    id = db.Column(db.Integer, primary_key=True)
    wind_code_p = db.Column(db.String(20), db.ForeignKey('fund_info.wind_code'))
    wind_code_s = db.Column(db.VARCHAR(20))
    date_adj = db.Column(db.Date)
    invest_scale = db.Column(MyReal)

    def __repr__(self):
        return self.wind_code_s


class FUND_ESSENTIAL(db.Model):
    __tablename__ = 'fund_essential_info'
    __searchable__ = ['wind_code_s', 'sec_name_s']
    wind_code_s = db.Column(db.VARCHAR(20), primary_key=True)
    wind_code = db.Column(db.VARCHAR(20))
    sec_name_s = db.Column(db.VARCHAR(100), unique=True)
    date_start = db.Column(db.Date)
    date_end = db.Column(db.Date)
    warning_line = db.Column(MyReal)
    winding_line = db.Column(MyReal)
    closed_period = db.Column(MyReal)
    investment_scope = db.Column(db.VARCHAR(5000))
    investment_restrictions = db.Column(db.VARCHAR(5000))
    open_frequency = db.Column(db.VARCHAR(50))
    bonus_mode = db.Column(db.VARCHAR(50))
    subscribe_threshold = db.Column(db.VARCHAR(100))
    redemption_threshold = db.Column(db.VARCHAR(100))
    day_count_4_calc_fee = db.Column(db.Integer,default=365)
    manage_fee_rate = db.Column(MyReal)
    manage_fee_calc_mode = db.Column(db.Integer)
    custodian_fee_rate = db.Column(MyReal)
    custodian_fee_calc_mode = db.Column(db.Integer)
    admin_fee_rate = db.Column(MyReal)
    admin_fee_calc_mode = db.Column(db.Integer)
    storage_fee_rate = db.Column(MyReal)
    storage_fee_calc_mode = db.Column(db.Integer)
    subscribe_fee_rate = db.Column(MyReal)
    redemption_fee_rate = db.Column(MyReal)
    subscribe_fee_mode = db.Column(db.Integer,default=0)
    incentive_raito = db.column(db.VARCHAR(400))
    incentive_mode = db.Column(db.VARCHAR(500))
    incentive_period = db.Column(db.VARCHAR(500))
    other_contract = db.Column(db.VARCHAR(5000))
    remark = db.Column(db.VARCHAR(2000))
    invest_amount = db.Column(MyReal)
    share_confirmed = db.Column(MyReal)


    def __repr__(self):
        return self.wind_code_s

    def to_json(self):
        d = {}
        for k,v in self.__dict__.items():
            if v is None:
                d[k] = ''
            else:
                d[k] = v
        return d



class FUND_STG_PCT(db.Model):
    __tablename__ = 'fund_stg_pct'
    id = db.Column(db.Integer, primary_key=True)
    wind_code = db.Column(db.String(20), db.ForeignKey('fund_info.wind_code'))
    stg_code = db.Column(db.String(20))
    trade_date = db.Column(db.Date)
    stg_pct = db.Column(MyReal)

    def __repr__(self):
        return self.stg_code


class FUND_NAV(db.Model):
    __tablename__ = "fund_nav"
    wind_code = db.Column(db.String(20), primary_key=True)
    nav_date = db.Column(db.Date, primary_key=True)
    nav = db.Column(MyReal)
    nav_acc = db.Column(MyReal)
    source_mark = db.Column(db.Integer)
    nav_tot = db.Column(MyReal)

    def __repr__(self):
        return self.wind_code


    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


@event.listens_for(FUND_NAV, 'after_insert')
def receive_after_insert(mapper, connection, target):
    "listen for the 'after_insert' event"
    #fund = FoFModel.query.filter_by(wind_code=target.wind_code).first()
    fund_name = check_code_order(target.wind_code)
    nav_acc = target.nav_acc
    nav_date = target.nav_date
    user = 'chun.wang@foriseinvest.com'
    send_email(user,subject="%s净净值已更新" %fund_name,template='email/new_acc',
               sec_name=fund_name,nav_acc=nav_acc,nav_date=nav_date)


class FUND_SEC_PCT(db.Model):
    '''资产信息'''
    __tablename__ = 'fund_sec_pct'
    id = db.Column(db.Integer, primary_key=True)
    wind_code = db.Column(db.String(20), nullable=False)
    sec_code = db.Column(db.String(20), nullable=False)
    nav_date = db.Column(db.Date, nullable=False)
    direction = db.Column(db.Integer, nullable=False)
    position = db.Column(db.Integer)
    cost_unit = db.Column(MyReal)
    cost_tot = db.Column(MyReal)
    cost_pct = db.Column(MyReal)
    value_tot = db.Column(MyReal)
    value_pct = db.Column(MyReal)
    trade_status = db.Column(db.String(200))
    sec_type = db.Column(db.Integer)

    def __repr__(self):
        return self.wind_code


class strategy_index_val(db.Model):
    __tablename__ = 'strategy_index_val'

    index_name = db.Column(db.VARCHAR, primary_key=True)
    nav_date = db.Column(db.Date, primary_key=True)
    value = db.Column(MyReal)

    def __repr__(self):
        return "%s" % self.index_name


class FUND_EVENT(db.Model):
    __tablename__ = 'fund_event'
    id = db.Column(db.INT, primary_key=True)
    wind_code = db.Column(db.String(20), db.ForeignKey('fund_info.wind_code'))
    event_date = db.Column(db.Date, nullable=False)
    event_type = db.Column(db.VARCHAR(20), nullable=False)
    remind_date = db.Column(db.Date, nullable=False)
    create_date = db.Column(db.Date)
    handle_status = db.Column(db.Boolean, default=0)
    description = db.Column(db.Text)
    color = db.Column(db.VARCHAR(20))
    create_user = db.Column(db.VARCHAR(45))
    private = db.Column(db.BOOLEAN)

    def __repr__(self):
        return "{} {} {}".format(self.wind_code, self.event_type, self.remind_date)


class PCT_SCHEME(db.Model):
    __tablename__ = 'scheme_fund_pct'
    id = db.Column(db.INT, primary_key=True)
    scheme_id = db.Column(db.VARCHAR(20), db.ForeignKey('scheme_info.scheme_id'))
    wind_code = db.Column(db.VARCHAR(20))
    invest_scale = db.Column(MyReal)

    def __repr__(self):
        return "{}".format(self.wind_code)





class INFO_SCHEME(db.Model):
    __tablename__ = 'scheme_info'
    scheme_id = db.Column(db.INT, primary_key=True)
    scheme_name = db.Column(db.VARCHAR(100))
    scheme_setupdate = db.Column(db.DateTime)
    create_user = db.Column(db.Integer)
    wind_code_p = db.Column(db.VARCHAR(20))
    backtest_period_start = db.Column(db.Date)
    backtest_period_end = db.Column(db.Date)
    return_rate = db.Column(MyReal)
    calmar_ratio = db.Column(MyReal)
    MDD = db.Column(MyReal)
    CAGR = db.Column(MyReal)
    ann_volatility = db.Column(MyReal)
    ann_downside_volatility = db.Column(MyReal)
    max_loss_monthly = db.Column(MyReal)
    max_profit_monthly = db.Column(MyReal)
    final_value = db.Column(MyReal)
    mdd_max_period = db.Column(MyReal)
    profit_loss_ratio = db.Column(MyReal)
    sortino_ratio = db.Column(MyReal)
    max_loss_weekly = db.Column(MyReal)
    max_profit_weekly = db.Column(MyReal)
    win_ratio = db.Column(MyReal)
    pct_scheme = db.relationship('PCT_SCHEME', backref='scheme_info', lazy='dynamic')

    def __repr__(self):
        return "{}".format(self.scheme_id)



class Invest_corp(db.Model):
    __tablename__ = 'fund_mgrcomp_info'
    mgrcomp_id = db.Column(db.Integer,primary_key=True)
    name = db.Column(db.VARCHAR(100))
    alias = db.Column(db.VARCHAR(100))
    review_status = db.Column(db.Integer)
    fund_count_tot = db.Column(db.Integer)
    fund_count_existing = db.Column(db.Integer)
    fund_count_active = db.Column(db.Integer)
    address = db.Column(db.VARCHAR(500))
    description = db.Column(db.VARCHAR(5000))
    registered_capital = db.Column(db.Integer)
    invest_corp_name = db.relationship('Invest_corp_file', backref='fund_mgrcomp_info', lazy='dynamic')

    def __repr__(self):
        return "{}".format(self.name)



class Invest_corp_file(db.Model):
    __tablename__ = 'fund_mgrcomp_file'
    file_id = db.Column(db.Integer,primary_key=True)
    mgrcomp_id = db.Column(db.Integer,db.ForeignKey('fund_mgrcomp_info.mgrcomp_id'))
    file_type = db.Column(db.VARCHAR(45))
    upload_user_id = db.Column(db.Integer)
    upload_datetime = db.Column(db.DateTime)
    file_name = db.Column(db.VARCHAR(500))
    file_content = db.Column(db.BLOB)
    comments = db.Column(db.Text)

    def __repr__(self):
        return "{}".format(self.file_name)


class FUND_NAV_CALC(db.Model):
    __tablename__ = 'fund_nav_calc'
    wind_code = db.Column(db.VARCHAR(20),primary_key=True)
    nav_date = db.Column(db.DATE,primary_key=True)
    share = db.Column(MyReal)
    market_value  = db.Column(MyReal)
    cash_amount = db.Column(MyReal)
    manage_fee = db.Column(MyReal)
    custodian_fee = db.Column(MyReal)
    admin_fee = db.Column(MyReal)
    storage_fee = db.Column(MyReal)
    other_fee = db.Column(MyReal)
    nav = db.Column(MyReal)
    cap_tot = db.Column(MyReal)
    cap_net = db.Column(MyReal)

    def __repr__(self):
        return "{}".format(self.wind_code)


    def to_json(self):
        d = {}
        for k,v in self.__dict__.items():
            print(type(v), v)
            if v is None:
                d[k] = ''
            elif isinstance(v,float):
                print(v)
            else:
                d[k] = v
        return d

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}



class FUND_TRANSACTION(db.Model):
    __tablename__ = "fund_transaction"
    id = db.Column(db.INTEGER,primary_key=True)
    fof_name =  db.Column(db.String(100),nullable=False)
    sec_name_s =  db.Column(db.String(100),nullable=False)
    wind_code_s = db.Column(db.String(45),nullable=False)
    operating_type = db.Column(db.String(45),nullable=False)
    accounting_date = db.Column(db.Date,nullable=False)
    request_date = db.Column(db.Date)
    confirm_date = db.Column(db.Date,nullable=False)
    confirm_benchmark = db.Column(MyReal)
    share = db.Column(MyReal)
    amount = db.Column(MyReal)
    description = db.Column(MyReal)

    def __str__(self):
        return  self.wind_code_s


def query_invest(rank):
    invest = Invest_corp.query.filter_by(review_status=rank).all()
    length = len(invest)
    if length  > 0 :
        invest_list  = [{"id": index, "name": i.name, "alias": i.alias, "review_status": i.review_status,
                         'tot':i.fund_count_tot,'existing':i.fund_count_existing,"active":i.fund_count_active,"uid":i.mgrcomp_id} for index, i in
                      enumerate(invest)]
        return {"data":invest_list,"length":length}
    else:
        return {"length":length,"data":""}



def check_code_order(wind_code):
    fof = FoFModel.query.filter_by(wind_code=wind_code).first()
    if fof is None:
        fof_mapping = FUND_ESSENTIAL.query.filter_by(wind_code_s=wind_code).first()
        sec_name = fof_mapping.sec_name_s
        return sec_name
    else:
        return fof.sec_name



def code_get_name(code: str) -> object:
    if code == '000300.SH':
        return "沪深300"
    fof = FoFModel.query.filter_by(wind_code=code).first()
    if fof is None:
        fof_mapping = FUND_ESSENTIAL.query.filter_by(wind_code_s=code).first()
        if fof_mapping is None:
            logger.warning("没有在fund_info中找到{}返回none".format(code))
        else:
            fof = FoFModel.query.filter_by(wind_code=fof_mapping.wind_code).first()
            if current_user.is_staff:
                return fof_mapping.sec_name_s
            else:
                return fof.alias
    else:
        if current_user.is_staff:
            return fof.sec_name
        elif fof.alias is None:
            logger.warning("{} 没有别名返回 {}".format(code, fof.sec_name))
            return fof.sec_name
        else:
            logger.info("{} 返回 {}".format(code, fof.sec_name))
            return fof.alias


def get_all_fof(user=current_user):
    fof = user.fofs
    if len(fof) > 0:
        fof_list = []
        for i in fof:
            all_child = FOF_FUND_PCT.query.filter_by(wind_code_p=i.wind_code).first()
            child = []
            if all_child is not None:
                latest_date = FOF_FUND_PCT.query.filter_by(wind_code_p=i.wind_code).order_by(
                    FOF_FUND_PCT.date_adj.desc()).first().date_adj
                child_fof = FOF_FUND_PCT.query.filter(
                    and_(FOF_FUND_PCT.wind_code_p == i.wind_code, FOF_FUND_PCT.date_adj == latest_date,
                         FOF_FUND_PCT.wind_code_s != 'fh0000')).all()
                for c in child_fof:
                    c_fund_mapping = FUND_ESSENTIAL.query.filter_by(wind_code_s=c.wind_code_s).first()
                    data = {}
                    data['name'] = code_get_name(c.wind_code_s)
                    data['date'] = c_fund_mapping.date_start.strftime("%Y-%m-%d")
                    data['scale'] = c.invest_scale
                    data['code'] = c.wind_code_s
                    latest_date_nav = FUND_NAV.query.filter_by(wind_code=c.wind_code_s) \
                        .order_by(FUND_NAV.nav_date.desc()).first()
                    if latest_date_nav is None:
                        data['nav_acc_latest'] = None
                        data['nav_date_latest'] = None
                    else:
                        data['nav_acc_latest'] = latest_date_nav.nav_acc
                        data['nav_date_latest'] = latest_date_nav.nav_date
                    child.append(data)
            fof_list.append({"primary": i, "child": child})
        return fof_list
    else:
        return None
