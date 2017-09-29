# -*- coding: utf-8 -*-
"""
:copyright: (c) 2016 by Huang Dong 
:mail: bjdhuang@cn.ibm.com.
:license: Apache 2.0, see LICENSE for more details.
"""


from os import path
from flask import flash,url_for,redirect,render_template,Blueprint,request
from fof_app.forms import LoginForm,RegisterForm,ChangePasswordForm,PasswordResetForm,PasswordResetRequestForm
from fof_app.models import  db,UserModel,get_all_fof
from flask_login import login_user,logout_user,current_user,login_required
from ..extensions import send_email
import logging

logger = logging.getLogger()

auth_blueprint = Blueprint('auth',__name__,template_folder=path.join(path.pardir,'templates','auth'))



@auth_blueprint.before_app_request
def before_request():
    if current_user.is_authenticated:
        #current_user.ping()
        if not current_user.confirmed \
                and request.endpoint \
                and request.endpoint[:5] != 'auth.' \
                and request.endpoint != 'static':
            return redirect(url_for('auth.unconfirmed'))

@auth_blueprint.route('/')
@login_required
def index():
    return  redirect(url_for("f_app.home"))


@auth_blueprint.route('/unconfirmed')
@login_required
def unconfirmed():
    if current_user.confirmed:
        return redirect(url_for('f_app.home'))
    return render_template('unconfirmed.html')

@auth_blueprint.route('/confirm')
@login_required
def resend_confirmation():
    token = current_user.generate_confirmation_token()
    send_email(current_user.email, '请激活你的账号',
               'email/confirm', user=current_user, token=token)
    flash('新的确认邮件已经发送到你的邮箱.')
    return redirect(url_for('f_app.home'))

@auth_blueprint.route('/confirm/<token>')
@login_required
def confirm(token):
    if current_user.confirmed:
        return redirect(url_for('f_app.home'))
    if current_user.confirm(token):
        flash('你的账号已激活')
    else:
        flash('这个激活链接已经过期')
    return redirect(url_for('f_app.home'))


@auth_blueprint.route('/change-password', methods=['GET', 'POST'])
@login_required
def change_password():
    fof_list = get_all_fof()
    form = ChangePasswordForm()
    if form.validate_on_submit():
        if current_user.verify_password(form.old_password.data):
            current_user.password = form.password.data
            db.session.add(current_user)
            db.session.commit()
            flash("密码修改成功")
            return redirect(url_for('f_app.home'))
        else:
            flash('无效的密码')
    return render_template("change_password.html", form=form,fof_list=fof_list)

@auth_blueprint.route('/reset', methods=['GET', 'POST'])
def password_reset_request():
    if not current_user.is_anonymous:
        return redirect(url_for('f_app.home'))
    form = PasswordResetRequestForm()
    if form.validate_on_submit():
        user = UserModel.query.filter_by(email=form.email.data).first()
        if user:
            token = user.generate_reset_token()
            send_email(user.email, '重置你的密码',
                       'email/reset_password',
                       user=user, token=token,
                       next=request.args.get('next'))
        flash('重置密码的邮件已经发送到邮箱')
        return redirect(url_for('auth.login'))
    return render_template('reset_password.html', form=form)


@auth_blueprint.route('/reset/<token>', methods=['GET', 'POST'])
def password_reset(token):
    if not current_user.is_anonymous:
        return redirect(url_for('f_app.home'))
    form = PasswordResetForm()
    if form.validate_on_submit():
        user = UserModel.query.filter_by(email=form.email.data).first()
        if user is None:
            return redirect(url_for('f_app.home'))
        if user.reset_password(token, form.password.data):
            flash('你的密码已经更新.')
            return redirect(url_for('auth.login'))
        else:
            return redirect(url_for('f_app.home'))
    return render_template('reset_password_token.html', form=form,token=token)



@auth_blueprint.route('/login', methods=['GET', 'POST'])
def login():
    form = LoginForm()
    if form.validate_on_submit():
        logger.info("用户名{}开始登录".format(form.email.data))
        user = UserModel.query.filter_by(email=form.email.data).first()
        if user is not None and user.verify_password(form.password.data):
            logger.info("用户{}登录成功,跳转至主页".format(user.username))
            login_user(user, form.remember_me.data)
            return redirect(request.args.get('next') or url_for('f_app.home'))
        else:
            logger.error("用户{}登录失败".format(form.email.data))
            flash('请检查用户名或密码')
    return render_template('login.html', form=form)



@auth_blueprint.route('/logout',methods=['GET','POST'])
@login_required
def logout():
    logout_user()
    flash("退出成功",category='success')
    return redirect(url_for('f_app.home'))

@auth_blueprint.route('/register',methods=['GET','POST'])
def register():
    form = RegisterForm()
    if form.validate_on_submit():
        new_user = UserModel(username=form.username.data,password=form.password.data)
        db.session.add(new_user)
        db.session.commit()
        flash("账号创建成功",category='success')
        return redirect(url_for('f_app.home'))
    return render_template('register.html',form=form)






