# encoding=utf8
import time
import smtplib
import boto3
from botocore.exceptions import ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header
from loguru import logger

from utils.template import render_template
from config import MAIL_CONFIG, EMAIL_BLACKLIST

# email
def send_email(to_email, subject, context):
    start_time = time.time()
    retry = True
    while retry:
        if (time.time() - start_time) > 22:
            logger.error(f"ERROR: Sending timeout! - {to_email} - {subject}")
            return {"code": 200, "success": False, "msg": "ERROR: Sending timeout"}
        try:
            today = time.strftime("%Y-%m-%d", time.localtime())
            logger.debug(f"today: {today}")

            # 发件人黑名单
            logger.debug(f"EMAIL_BLACKLIST: {EMAIL_BLACKLIST}")
            from_emails=MAIL_CONFIG['userlist']
            # logger.debug(f"from_emails: {from_emails}")
            if str(EMAIL_BLACKLIST).find(today) > 0:
                for black_mail in EMAIL_BLACKLIST[today]:
                    if str(from_emails).find(black_mail) > 0:
                        from_emails.remove(black_mail)
            from_emails_len = len(from_emails)
            logger.debug(f"from_emails: {from_emails} {from_emails_len}")
            if from_emails_len < 1:
                logger.error(f"ERROR: Not enough senders! - {to_email} - {subject}")
                return {"code": 400, "success": False, "msg": "ERROR: Not enough senders"}
                break
            from_email=from_emails[0]
            logger.debug(f"from_email: {from_email}")

            # 创建MIME文本对象
            # message = MIMEText(context, "plain", 'utf-8')
            message = MIMEText(context, "html", 'utf-8')
            # 设置邮件头
            message['Subject'] = Header(subject, 'utf-8')
            message['From'] = f"Mail Service <{from_email}>"    # 设置发送者信息
            # message['From'] = from_email  #Header(from_addr, 'utf-8')
            message['To'] = to_email      #Header(to_email, 'utf-8')

            # 创建SMTP连接
            if MAIL_CONFIG['port'] == 25:
                # 使用非加密方式连接
                smtpObj = smtplib.SMTP(MAIL_CONFIG['host'], MAIL_CONFIG['port'])
            elif MAIL_CONFIG['port'] == 465:
                # 使用SSL方式连接,
                smtpObj = smtplib.SMTP_SSL(MAIL_CONFIG['host'])
            elif MAIL_CONFIG['port'] == 587:
                # 使用TLS方式连接
                smtpObj = smtplib.SMTP(MAIL_CONFIG['host'], MAIL_CONFIG['port'])
                # smtpObj.ehlo()
                smtpObj.starttls()
            # debug日志
            # smtpObj.set_debuglevel(1)
            # 登录发送邮件服务器
            smtpObj.login(from_email, MAIL_CONFIG['password'])
            # 发送邮件
            smtpObj.sendmail(from_email, to_email, message.as_string())

            logger.success(f"{subject} {from_email} => {to_email} Email sent successfully")

            # 关闭SMTP连接
            smtpObj.quit()
            return {"code": 200, "success": True, "msg": "Email sent successfully"}
            break
        except UnicodeEncodeError as e:  # 'ascii' codec can't encode character '\xec' in position 10: ordinal not in range(128)
            logger.error(f"{subject} {from_email} => {to_email} Email sent failed: {e}")
            return {"code": 200, "success": False, "msg": "Email sent failed"}
        except Exception as e:
            if str(e).find('sending limit') > 0:  # User xxx@xxx.net has exceeded its 24-hour sending limit. Messages to 500 recipients out of 500 allowed have been sent. Relay quota will reset in 18.49 hours.
                # 发送失败添加到发件人黑名单
                logger.info(f"{today} Add sender to blacklist: {from_email} {e}")
                if str(EMAIL_BLACKLIST).find(today) > 0:
                    blacktoday = EMAIL_BLACKLIST[today]
                    blacktoday.append(from_email)
                    EMAIL_BLACKLIST[today] = blacktoday
                else:
                    values = [from_email]
                    EMAIL_BLACKLIST[today] = values
                logger.debug(f"EMAIL_BLACKLIST: {EMAIL_BLACKLIST}")
            elif str(e).find('unexpectedly closed') > 0:  # Connection unexpectedly closed
                # 发送失败添加到发件人黑名单
                logger.info(f"{today} Add sender to blacklist: {from_email} {e}")
                if str(EMAIL_BLACKLIST).find(today) > 0:
                    blacktoday = EMAIL_BLACKLIST[today]
                    blacktoday.append(from_email)
                    EMAIL_BLACKLIST[today] = blacktoday
                else:
                    values = [from_email]
                    EMAIL_BLACKLIST[today] = values
                logger.debug(f"EMAIL_BLACKLIST: {EMAIL_BLACKLIST}")
            elif str(e).find('exceeded allowed number') > 0:  # Domain xxx.net has exceeded allowed number of recipients for the current time period
                # 发送失败添加到发件人黑名单
                logger.info(f"{today} Add sender to blacklist: {from_email} {e}")
                if str(EMAIL_BLACKLIST).find(today) > 0:
                    blacktoday = EMAIL_BLACKLIST[today]
                    blacktoday.append(from_email)
                    EMAIL_BLACKLIST[today] = blacktoday
                else:
                    values = [from_email]
                    EMAIL_BLACKLIST[today] = values
                logger.debug(f"EMAIL_BLACKLIST: {EMAIL_BLACKLIST}")
            elif str(e).find('too many messages sent') > 0:  # IP x.x.x.x temporarily rejected for too many messages sent. Please check any clients or devices that may be misconfigured, and try again later.
                logger.error(f"{subject} {from_email} => {to_email} Email sent failed: {e}")
                return {"code": 400, "success": False, "msg": "Email sent failed"}
            else:
                logger.error(f"{subject} {from_email} => {to_email} Email sent failed: {e}")
                return {"code": 400, "success": False, "msg": "Email sent failed"}

def send_activation_mail(to_email, username, verify_url):
    body_text = f"This is the registration email, please open the link to continue. {verify_url}"
    body_html = render_template('utils/templates/activation_account_mail.html',{
                    'username':username,
                    'domain':verify_url
                })
    # logger.debug(f"{to_email} => {email_message}")

    # Send activation email
    return send_email(
            to_email,
            f"Account Creation Notification",
            body_html
        )

def send_reset_mail(to_email, username, reset_url):
    body_text = f"Attention: Password reset operation is in progress, please open the link to continue. {reset_url}"
    body_html = render_template('utils/templates/reset_password_mail.html',{
                    'username':username,
                    'domain':reset_url
                })
    # logger.debug(f"{to_email} => {email_message}")

    # Send reset email
    return send_email(
            to_email, 
            f"Account Reset Notification",
            body_html
        )

def send_newpasswd_mail(to_email, username, password):
    body_text = f"Attention: The account password has been changed, new password is {password}"
    body_html = render_template('utils/templates/new_password_mail.html',{
                    'username':username,
                    'password':password
                })
    # logger.debug(f"{to_email} => {email_message}")

    # Send newpasswd email
    return send_email(
            to_email, 
            f"Account Password Notification",
            body_html
        )

def send_normal_mail(to_email, subject, context):
    body_text = f"{context}"
    body_html = f"""\
    <html>
    <head></head>
    <body>
    <p>{context}</p>
    </body>
    </html>
    """
    # logger.debug(f"{to_email} => {email_message}")

    # Send newpasswd email
    return send_email(
            to_email, 
            subject,
            body_html
        )
