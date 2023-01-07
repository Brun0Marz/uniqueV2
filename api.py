from distutils.log import debug
from flask import Flask,jsonify,request,make_response

from mongoDBqueries import *
from id_generator import generate_id
from str_to_bool import str2bool

app = Flask(__name__)

@app.route('/check_account', methods=['GET'])
def check_account():
    account_number = int(request.args.get('accNum'))

    #Check if account exists

    if checkAccountExists(accountNumber=account_number):

        return jsonify({
            'account_exists':True,
                        })
    else:
        return jsonify({
            'account_exists':False,
                        })


@app.route('/process_id', methods=['GET'])
def id_processing_exists():

    ip_address=request.environ['REMOTE_ADDR']

    id_code = request.args.get('id_code')
    referral_url=request.args.get('referral_url')
    session_id=request.args.get('session_id')

    print(session_id)

    account_number = int(request.args.get('accNum'))

        #Return visitor, with a code that could be theirs or someone else's, or no code at all.
    if check_id_exists(id_code=id_code,account_number=account_number):
        print('Reused code for IP')
        #So we have them using one of their old codes. Record this and issue a new one.
        newCode=str(account_number)+'-'+generate_id()

        write_new_to_db(ip_address=ip_address,code_old=id_code,code_new=newCode,session_id=session_id,isFirst=False,wasReferred=True,page_url=referral_url,account_number=account_number)

    else:
        #Previous visitor, referred again by someone else, or no code at all.
        print('Old Visitor, using a new code though.')
        newCode=str(account_number)+'-'+generate_id()
        if id_code is not None:
            write_new_to_db(ip_address=ip_address,code_old=id_code,code_new=newCode,session_id=session_id,isFirst=False,wasReferred=False,code_reused=True,page_url=referral_url,account_number=account_number)
        else:
            write_new_to_db(ip_address=ip_address,code_old=None,code_new=newCode,session_id=session_id,isFirst=True,wasReferred=False,code_reused=None,page_url=referral_url,account_number=account_number)

    response = make_response(
                jsonify(
                    {'id_code': newCode}
                ),
                200,
            )
    response.headers["Content-Type"] = "application/json"
    response.headers.add('Access-Control-Allow-Origin', '*')

    return response


@app.route('/get_most_used_id_codes', methods=['GET'])
def get_most_used_id_codes():
    account_number = int(request.args.get('acc_num'))
    count_return=int(request.args.get('num_return'))
    count_dict=str2bool(request.args.get('return_dict'))
    print(count_dict)
    time_window=request.args.get('time_frame')
    post_process_only=bool(request.args.get('post_process'))

    codes=get_most_used_codes(account_number=account_number,count_return=count_return,return_count_dict=count_dict,time_window=time_window,post_process_only=post_process_only)

    response = make_response(
                jsonify(
                    codes
                ),
                200,
            )
    response.headers["Content-Type"] = "application/json"
    response.headers.add('Access-Control-Allow-Origin', '*')

    return response


if __name__ == '__main__':
    app.run(host='192.168.0.190', port=8000,debug=True)