import requests
import pandas as pd
import numpy as np
#from requests_ntlm import HttpNtlmAuth
import json
from multiprocessing.dummy import Pool as ThreadPool
from  functools  import partial
#import numpy as np

def get_pswd(pswd_path):
    with open(pswd_path) as f:
        pswd=json.load(f)['AD']
    return tuple(pswd)

#r'http://jira.moscow.alfaintra.net'
class jira_parser():
    
    def __init__(self,pswd_tuple,server=r'http://jira',api='/rest/api/2/',bulk_size=8,session_life_cycle=5):
        self.__pswd=pswd_tuple
        self.server=server
        self.api_address=server+api
        
        self.headers={
            'Cache-Control': 'no-cache',
            # 'Accept': 'application/json;charset=UTF-8',  # default for REST
            'Content-Type': 'application/json',  # ;charset=UTF-8',
            # 'Accept': 'application/json',  # default for REST
            # 'Pragma': 'no-cache',
            # 'Expires': 'Thu, 01 Jan 1970 00:00:00 GMT'
            'X-Atlassian-Token': 'no-check'}

        
        self.user_agent='Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36'
        self.bulk_size=bulk_size
        self.session_life_cycle=session_life_cycle
        self.tasks_retrive_limit=1000
        
        self.query_params= {'expand':'changelog'}
    
    def __parse_response(self,response):
        try:
            res=response.json()
        except:
            try:
                res=response.text()
            except:
                res=response
        return res
        
    def __create_session(self):
        session =requests.session()
        session.auth =self.__pswd #HttpNtlmAuth('***', '***')
        session.headers=self.headers
        #session.headers.update({'User-Agent': self.user_agent})
        #session.verify = False
        #session.cert=None
        return session

    def __task_retrive(self,session,full_task_name):
        """full_task_name = <project>-<id>
        e.g. 'SFAREPORT-200'
        """
        return session.get(self.api_address+full_task_name,params=self.query_params)
        
    def bulk_retrieve_tasks(self,tasks_list):
        n=len(tasks_list)
        res=[]
        chunk_size=self.bulk_size*self.session_life_cycle
  
        for big_chunk in (tasks_list[i:i+chunk_size] for i in range(0,n,chunk_size)):
            s=self.__create_session()
            #print(self.__task_retrive(s,'issue/SFAREPORT-208'))
            worker=partial(self.__task_retrive, s)
            
            for chunk in (big_chunk[i:i+self.bulk_size] for i in range(0,len(big_chunk),self.bulk_size)):
                pool = ThreadPool()
                
                results = pool.map(worker, chunk)
                pool.close()
                pool.join()
                res += results
        return res
    @staticmethod
    def prepare_jira_tasks(jira_project,task_id):
        return 'issue/{0}-{1}'.format(jira_project,task_id)
    
    def get_all_tasks_from_project(self,project):
        jira_task=partial(jira_parser.prepare_jira_tasks,project)
        count=1
        r_code=200
        res=[]
        while r_code==200:
            if count%(self.bulk_size*self.session_life_cycle)==1:
                s=self.__create_session()
                worker=partial(self.__task_retrive, s)
            
            next_count=count+self.bulk_size
            chunk=list(map(jira_task,range(count,next_count,1)))
            
            pool = ThreadPool()
            results = pool.map(worker, chunk)
            pool.close()
            pool.join()
            res += results           
            
            count =next_count
            assert(count<self.tasks_retrive_limit)
            r_code=results[-1].status_code
  
        k=[el.status_code for el in res[-self.bulk_size-2:]][::-1].index(200)
        return res[:-k]
    
    def get_sys_info(self,project,default_task=1):
        request= self.api_address+r'issue/{0}-{1}/transitions'.format(project,default_task)
        s=self.__create_session()
        transaction_types=s.get(request,params=self.query_params)
        
        transaction_types=pd.DataFrame(transaction_types.json()['transitions'])
        return transaction_types
    
    def update_status(self,issue_name,transition, fields=None, comment=None, worklog=None, **fieldargs):
        request=self.api_address+r'issue/{0}/transitions'.format(issue_name)
        
        s=self.__create_session()
       
        data = {'transition': {'id': transition}}
        if comment:
            data['update'] = {'comment': [{'add': {'body': comment}}]}
        if worklog:
            data['update'] = {'worklog': [{'add': {'timeSpent': worklog}}]}
        if fields is not None:
            data['fields'] = fields
        else:
            fields_dict = {}
            for field in fieldargs:
                fields_dict[field] = fieldargs[field]
            data['fields'] = fields_dict
        return s.post(request,data=json.dumps(data))
        
    def create_issue_link(self,issue_a,issue_b,link_type_name='Зависимость'):
        request=self.api_address +r'issueLink'
        s=self.__create_session()
        data={'type':{'name':link_type_name},
              'inwardIssue':{'key':issue_a},
              'outwardIssue':{'key':issue_b},
             }
        return s.post(request,data=json.dumps(data))
    
    def create_issue(self,summary='',description='',assignee='',issue_type='Task',project='SFAREPORT'):
        request=self.api_address+r'issue'
        s=self.__create_session()
        data={'fields': {'assignee': {'name': assignee},
                         'description': description,
                         'issuetype': {'name': issue_type},
                         'project': {'key': project},
                         'summary': summary}}
        response=s.post(request,data=json.dumps(data))      
        return self.__parse_response(response)
    
    def add_comment(self,issue_name,comment):
        request=self.api_address +r'issue/{}/comment'.format(issue_name)
        s=self.__create_session()
        data={'body':comment}
        return s.post(request,data=json.dumps(data))
		
class jira_reports():

    def tryconvert(obj,stringcmd):  
        try:
            res=eval(stringcmd)
        except:
            res=''
        return res


    def get_task_history(task_json):

        created=pd.to_datetime(task_json['fields']['created'],
                                               infer_datetime_format=True)
        prev_date= {'prev_date':created}

        last_assignee=task_json['fields']['assignee'].get('displayName')
        task_id={'task_id':task_json['key']}
        history_list=task_json['changelog']['histories']
        status_info=[]
        assignee_info=[]

        for history in history_list:
            start_dt={'change_date':pd.to_datetime(history['created'],infer_datetime_format=True)}
            status_info += [{**el,**prev_date,**start_dt,**task_id}
                            for el in history['items'] if el['field'] in ['status']]
            assignee_info += [{**task_id,**el,**start_dt,} 
                              for el in history['items'] if el['field']=='assignee']

            prev_date['prev_date'] =start_dt['change_date']  
        #return status_info,task_json

        if len(status_info)==0: status_info=[{**{'fromString':task_json['fields']['status']['name']},
                                              **{'prev_date':created},
                                              **{'change_date':pd.datetime.now()},
                                              **task_id}]

        if len(assignee_info)==0: assignee_info=[{**{'fromString':last_assignee},**{'toString':last_assignee},
                                                  **{'change_date':prev_date['prev_date']},
                                                  **task_id}]

        status_info=pd.DataFrame(status_info)
        status_info.loc[0,'prev_date']=created

        status_info['status_time']=(status_info['change_date']-status_info['prev_date']).apply(
            lambda t:t.total_seconds()/3600/24)


        return status_info,pd.DataFrame(assignee_info)    
    
    
    def get_logins(tasks_df):
        res=[]
        l=('reporter','assignee')
        for i,row in tasks_df.iterrows():
            for el in l:
                try:
                    r=row.fields[el]
                    res.append({'login':r['name'],'fio':r['displayName']})
                except:
                    pass

        return pd.DataFrame(res).drop_duplicates().reset_index(drop=True)
    
    
    def create_report(df):
        report=df[['key']].copy(deep=True)
        report['issue_name']=df.fields.apply(lambda d:d['summary'])
        report['issue_type']=df.fields.apply(lambda d:d['issuetype']['name'])
        report['description']=df.fields.apply(lambda d:str(d['description']))
        report['business_owner']=df.fields.apply(lambda d:d['customfield_11686'])
        report['status']=df.fields.apply(lambda d:d['status']['name'])
        report['priority']=df.fields.apply(lambda d:d['priority']['name'])



        report['outwardIssues']=df.fields.apply(lambda d:','.join([issue['outwardIssue']['key'] 
                                                          for issue in d['issuelinks'] 
                                                          if 'outwardIssue' in issue.keys()]))

        report['outwardIssuesSumm']=df.fields.apply(lambda d:','.join([issue['outwardIssue']['fields']['summary']
                                                          for issue in d['issuelinks'] 
                                                          if 'outwardIssue' in issue.keys()]))

        report['inwardIssues']=df.fields.apply(lambda d:','.join([issue['inwardIssue']['key'] 
                                                          for issue in d['issuelinks'] 
                                                          if 'inwardIssue' in issue.keys()]))

        report['resolution']=df.fields.apply(lambda d:d.get('resolution').get('name') if d.get('resolution')!=None else None )
        report['resolutiondate']=df.fields.apply(lambda d:d['resolutiondate'])
        report['reporter']=df.fields.apply(lambda d:d['reporter']['displayName'])
        #report['assignee']=df.fields.apply(lambda d:tryconvert(d,"obj['assignee']['displayName']"))
        report['assignee']=df.fields.apply(lambda d:d.get('assignee').get('displayName') if d.get('assignee')!=None else None )
        report['duedate']=df.fields.apply(lambda d:pd.to_datetime(d['duedate'],infer_datetime_format=True))
        report['created']=df.fields.apply(lambda d:pd.to_datetime(d['created'],infer_datetime_format=True))
        report['updated']=df.fields.apply(lambda d:pd.to_datetime(d['updated'],infer_datetime_format=True))		
        report['creator']=df.fields.apply(lambda d:d['creator']['displayName'])


       

        report['plan_analys_beg_date']=df.fields.apply(lambda d:d['customfield_14770'])
        report['plan_analys_end_date']=df.fields.apply(lambda d:d['customfield_14771'])

        # дальше идет магия (исхожу из того, что комп мощный и много оперативы)

        temp=df[['changelog','fields','key']].apply(lambda row:jira_reports.get_task_history(row),axis=1)
        temp_stages=  temp.apply (lambda  el : pd.DataFrame(el[0].groupby('fromString')['status_time'].sum()).T)

        temp_stages=pd.concat(list(temp_stages),axis=0).add_prefix('Stage_').reset_index(drop=True)

        temp_actors=temp.apply (lambda  el : pd.DataFrame.from_dict(
            {key: [1] for key in el[1]['toString'].unique()},orient='columns'))

        temp_actors=pd.concat(list(temp_actors),axis=0).add_prefix('Actor_').reset_index(drop=True)
        report_columns=list(report.columns)+list(temp_actors.columns)+list(temp_stages.columns)
        report=pd.concat([temp_stages,temp_actors,report.reset_index(drop=True)],axis=1)[report_columns]
        report['time_till_now']=report['created'].apply (lambda t:(pd.datetime.now()-t))
        report['time_till_now_conv']=report['time_till_now'].apply (lambda t:t.total_seconds()/3600/24)
        try:
            report['volochkov_flg']=report[['reporter','Actor_Волочков Илья Сергеевич','creator']].apply(lambda row:1 if row[1]==1 or 
                                                                                          row[0]=='Волочков Илья Сергеевич' or
                                                                                        row[2]=='Волочков Илья Сергеевич'
                                                                                                     else 0,axis=1)
        except:
            pass

        report['final_status']=report.status.apply(lambda s:1 if s in ['Closed','Бой'] else 0)


        return report
