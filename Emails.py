alpaca = AlpacaAPI.initiate_API(live=LIVE)
azure = AzureAPI.initiate_API(live=LIVE)

def send_update_profile_emails():

    print('***Sending update profile emails.***')

    ids = alpaca.get_account_ids(active=True,funded=True)

    for id in ids:

        brokerage_account = alpaca.get_brokerage_account(id)
        
        needs_trusted_contact = 'trusted_contact' not in brokerage_account

        disclosures = brokerage_account['disclosures']
        employment_status = disclosures['employment_status']
        if employment_status == 'EMPLOYED':
            needs_employment = 'employer_name' not in disclosures
        else:
            needs_employment = False

        if needs_trusted_contact:
            additional_info = 'Additionally, we encourage you to provide the details of a trusted contact as you have not yet done so.'
        elif needs_employment:
            additional_info = 'Additionally, we encourage you to provide the details of your employment as you have not yet done so.'
        elif (needs_trusted_contact & needs_employment):
            additional_info = 'Additionally, we encourage you to provide the details of a trusted contact and your employment as you have not yet done so.'
        else:
            additional_info = ''

        user_info = azure.get_user_info(id)
        client_name = user_info.first_name
        client_email = user_info.email

        subject = 'Review Profile'
        body = """This is a monthly reminder to make sure the information on your profile is up to date.
                This information can influence how we manage your investments so it is crucial to ensure that it is both accurate and current. {additional_info}
                You can make changes to your profile by navigating to the "Profile" tab when logged into your account.""".format(additional_info=additional_info)

        hf.send_event_email(client_name,client_email,subject,body,LIVE)

    print('***Finished sending update profile emails.***')