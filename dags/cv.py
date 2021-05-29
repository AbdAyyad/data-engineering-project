from datetime import timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
import logging

import time

LOGGER = logging.getLogger("airflow.task")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
        'cv',
        default_args=default_args,
        description='ETL for data engineering project',
        schedule_interval=None,
        start_date=days_ago(2),
        tags=['ETL'],
) as dag:

    tags = {1:'Name',2:'Target_Job_Title',3:'Date_of_Birth',4:'Address',5: 'Nationality', 6: 'Phone_Number',7: 'Email',8: 'Social_Profiles',9: 'Marital_Status',10: 'Gender',11: 'Age',
            21:'Education_Institution_Name',22:'Education_Title',23:'Degree',24:'Education_Start_Date',25:'Education_End_Date',26:'Education_Duration',27:'GPA',
            41:'Company_Name',42:'Job_Title',43:'Job_Start_Date',44:'Job_End_Date',45:'Job_Duration',46:'Job_Location',47:'Company_Industry',48:'Job_Type',49:'Job_Duties',
            61:'Start_of_skill',62:'Middle_of_skill',63:'Language',
            81: 'Summary_Target_Job_Title',
            101:'Course_Name',102:'Certificate_Name',103:'Course_Duration',104: 'Course_Issued_Date',105: 'Course_Institution_Name', 106: 'Course_Summary',
            121:'Project_Name',122: 'Project_Description',123: 'Project_Issued_Date',124: 'Project_Role',125: 'Project_Type',126: 'Project_Client',
            141: 'Award_Title',142: 'Award_Issued_Date',143: 'Awarder',144: 'Award_Summary',
            161: 'Publication_Name',162: 'Publisher',163: 'Publication_Issued_Date',164: 'Publication_URL',165: 'Publication_Summary',
            181: 'Hobby',
            201: 'Reference_Name',202: 'Reference_Job_Title',203: 'Reference_Company_Name',204: 'Reference_Email',205: 'Reference_Phone_Number',206: 'Reference_Note'}

    listkeys = list(tags.keys())
    listvalues = list(tags.values())
    tagsreversed = {}
    for i in range(len(listvalues)):
        tagsreversed[listvalues[i]] = listkeys[i]

    # for i in range(len(all_cvs)):
    #     edsubg = list(all_cvs[i]['education'].keys())
    #     for k in edsubg:
    #         inner = list(all_cvs[i]['education'][k].keys())
    #         if 'Education_Date' in inner:
    #             del all_cvs[i]['education'][k]['Education_Date']
    #     exsubg = list(all_cvs[i]['experience'].keys())
    #     for k in exsubg:
    #         inner = list(all_cvs[i]['experience'][k].keys())
    #         if 'Job_Date' in inner:
    #             del all_cvs[i]['experience'][k]['Job_Date']
    #     cersubg = list(all_cvs[i]['certifications'].keys())
    #     for k in cersubg:
    #         inner = list(all_cvs[i]['certifications'][k])
    #         if 'Job_Date' in inner:
    #             del all_cvs[i]['certifications'][k]['Job_Date']

    neo4j_create_statement = """create (`90`:All_Skills {Name:'All Skills'}),(`91`:All_Job_Titles {Name:'All Job Titles'}),(`92`:All_Company_Names {Name:'All Company Names'}),(`93`:All_Education_Titles {Name:'All Education Titles'})
    ,(`94`:All_Course_Names {Name:'All Course Names'}),(`95`:All_Certification_Names {Name:'All Certification Names'}),(`96`:All_Course_Institutions {Name:'All Course Institutions'}),(`97`:All_Project_Names {Name:'All Project Names'})
    ,(`98`:All_Project_Roles {Name:'All Project Roles'}),(`99`:All_Project_Clients {Name:'All Project Clients'}),(`100`:All_Award_Names {Name:'All Award Names'}),(`101`:All_Awarders {Name:'All Awarders'})
    ,(`102`:All_Publication_Names {Name:'All Publication Names'}),(`103`:All_Publishers {Name:'All Publishers'}),(`104`:All_Institution_Names {Name:'All Institution Names'})"""

    # execution_commands = []
    # all_commands_elements = []
    # for i in range(10):

    prerequisite = BashOperator(
        task_id='prerequisite',
        bash_command='pip install neo4j elasticsearch'
    )

    def upload_cv(**kwargs):
        ti = kwargs['ti']
        url = "http://185.96.69.234:8000/upload"
        payload = {}
        file_name = kwargs['dag_run'].conf['file_id']+'.pdf'
        with open(kwargs['dag_run'].conf['path'], 'rb') as current_file:
            files = [
                ('files', (file_name,current_file , 'application/pdf'))
            ]
            headers = {
                'X-API-KEY': '92f023f8b3ea67d455b9398c3f51fc48'
            }
            response = requests.request("POST", url, headers=headers, data=payload, files=files)
            LOGGER.info('status code from upload api: ' + str(response.status_code))
            LOGGER.info('response from upload api: ' + str(response.json()[0]))
            if response.status_code != 200:
                raise ValueError('upload api return status code' + str(response.status_code))
            ti.xcom_push(key='parser_id', value=response.json()[0])


    t1 = PythonOperator(
        task_id='upload_cv',
        python_callable=upload_cv,
        provide_context=True,
        dag=dag,
    )


    def download_parsed_data(**kwargs):
        ti = kwargs['ti']
        parser_id = ti.xcom_pull(key='parser_id', task_ids=['upload_cv'])
        time.sleep(5)
        LOGGER.info('document id: ' + str(parser_id))
        url = "http://185.96.69.234:8000/parsed?id=" + parser_id[0]

        payload = {}
        headers = {
            'X-API-KEY': '92f023f8b3ea67d455b9398c3f51fc48'
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        LOGGER.info('status code from download api: ' + str(response.status_code))
        LOGGER.info('response from download api: ' + str(response.json()))
        if response.status_code != 200:
            raise ValueError('download api return status code' + str(response.status_code))
        file_name = '/home/spring/uploaded-cv/'+kwargs['dag_run'].conf['file_id']+'.json'
        LOGGER.info(file_name)
        with open(file_name,'w') as f:
            f.write(str(response.json()).replace('\'','\"'))


    t2 = PythonOperator(
        task_id='download_parsed_data',
        python_callable=download_parsed_data,
        provide_context=True,
        dag=dag,
    )

    def node_create_builder(cv):
        commands_elements = []
        personal_elements = []
        education_elements = []
        experience_elements = []
        certifications_elements = []
        projects_elements = []
        awards_elements = []
        publications_elements = []
        references_elements = []

        inner_elements = []
        skills_elements = []
        institution_elements = []
        education_title_elements = []
        job_title_elements = []
        company_elements = []
        course_name_elements = []
        certification_name_elements = []
        course_institution_elements = []
        project_name_elements = []
        project_role_elements = []
        project_client_elements = []
        award_title_elements = []
        awarder_elements = []
        publication_name_elements = []
        publisher_elements = []

        for_delete_lists = []
        skill_for_delete_list = []
        institution_for_delete_list = []
        education_title_for_delete_list = []
        job_for_delete_list = []
        company_for_delete_list = []
        course_name_for_delete_list = []
        certification_name_for_delete_list = []
        course_institution_for_delete_list = []
        project_name_for_delete_list = []
        project_role_for_delete_list = []
        project_client_for_delete_list = []
        award_title_for_delete_list = []
        awarder_for_delete_list = []
        publication_name_for_delete_list = []
        publisher_for_delete_list = []

        neo4j_create_statement = "create (`0`:Resume {Resume_Id:'" + str(cv['CV_ID']) + "'}),(`01`:Personal_Information {Name:'Personal Information'"
        dict_keys = list(cv['personal'].keys())

        for k in dict_keys:
            if isinstance(cv['personal'][k],list):
                try:
                    cv['personal'][k] = cv['personal'][k][0].strftime("%d %B %Y")
                except:
                    continue
            if str(k) == 'Name':
                neo4j_create_statement+= ",CV_"+ str(k) +":'" + str(cv['personal'][k]) + "'"
            else:
                neo4j_create_statement+= ","+ str(k) +":'" + str(cv['personal'][k]) + "'"
        neo4j_create_statement+="})"

        neo4j_create_statement+= ",(`02`:Education {Name:'Education'})"
        dict_keys = list(cv['education'].keys())
        for k in dict_keys:
            neo4j_create_statement+= ",(" + 'edsg'+str(k) + ":Education_Subgroup {Name:'Education_Subgroup',Resume_Id:'"+ str(cv['CV_ID']) + "',sg:'"+str(k)+"'"
            second_dict_keys = list(cv['education'][k].keys())
            only_nodes = ['Education_Institution_Name','Education_Title']
            for j in second_dict_keys:
                if isinstance(cv['education'][k][j],date):
                    cv['education'][k][j] = cv['education'][k][j].strftime("%d %B %Y")
                if j not in only_nodes:
                    neo4j_create_statement+= "," + str(j) + ":'" + str(cv['education'][k][j]) + "'"
            neo4j_create_statement+= "})"
            education_elements.append('edsg'+str(k))
            for j in second_dict_keys:
               if j in only_nodes and cv['education'][k][j] != '':
                   neo4j_create_statement+= ",(edsg"+str(k)+ str(tagsreversed[j]) +":" + j + ":" + str(cv['education'][k][j]).replace(' ','_').replace('&','and').replace('.','').replace('-','_') +" {Name:'" + str(cv['education'][k][j]) + "'})"
                   if j == only_nodes[0]:
                       institution_elements.append(str(cv['education'][k][j]).replace(' ','_').replace('&','and').replace('.','').replace('-','_'))
                       institution_for_delete_list.append('edsg'+str(k)+ str(tagsreversed[j]))
                   else:
                       education_title_elements.append(str(cv['education'][k][j]).replace(' ','_').replace('&','and').replace('.','').replace('-','_'))
                       education_title_for_delete_list.append('edsg'+str(k)+ str(tagsreversed[j]))


        neo4j_create_statement+= ",(`03`:Experience {Name:'Experience'})"
        dict_keys = list(cv['experience'].keys())
        for k in dict_keys:
            neo4j_create_statement+= ",(" + 'exsg'+str(k) + ":Experience_Subgroup {Name:'Experience_Subgroup',Resume_Id:'"+ str(cv['CV_ID']) + "',sg:'"+str(k)+"'"
            second_dict_keys = list(cv['experience'][k].keys())
            only_nodes = ['Company_Name','Job_Title']
            for j in second_dict_keys:
                if isinstance(cv['experience'][k][j],date):
                    cv['experience'][k][j] = cv['experience'][k][j].strftime("%d %B %Y")
                if j not in only_nodes:
                    neo4j_create_statement+= "," + str(j) + ":'" + str(cv['experience'][k][j]) + "'"
            neo4j_create_statement+= "})"
            experience_elements.append('exsg'+str(k))
            for j in second_dict_keys:
                if j in only_nodes and cv['experience'][k][j] != '':
                    neo4j_create_statement+= ",(exsg"+str(k)+ str(tagsreversed[j]) +":" + j + ":" + str(cv['experience'][k][j]).replace(' ','_').replace('&','and').replace('.','').replace('-','_') + " {Name:'" + str(cv['experience'][k][j]) + "'})"
                    if j == only_nodes[0]:
                        company_elements.append(str(cv['experience'][k][j]).replace(' ','_').replace('&','and').replace('.','').replace('-','_'))
                        company_for_delete_list.append('exsg'+str(k)+ str(tagsreversed[j]))
                    else:
                        job_title_elements.append(str(cv['experience'][k][j]).replace(' ','_').replace('&','and').replace('.','').replace('-','_'))
                job_for_delete_list.append('exsg'+str(k)+ str(tagsreversed[j]))

        neo4j_create_statement+= ",(`04`:Skills {Name:'Skills',Resume_Id:'"+ str(cv['CV_ID']) + "'})"
        dict_keys = cv['skills']
        for z,k in enumerate(dict_keys):
                neo4j_create_statement+= ",("+ 'skill'+str(z)+':'+ str(k).replace(' ','_').replace('&','and').replace('.','').replace('-','_') + " {Name:'" + str(k) + "'})"
                skills_elements.append(str(k).replace(' ','_').replace('&','and').replace('.','').replace('-','_'))
                skill_for_delete_list.append('skill'+str(z))

        neo4j_create_statement+= ",(`05`:Certifications {Name:'Certifications'})"
        dict_keys = list(cv['certifications'].keys())
        for k in dict_keys:
            neo4j_create_statement+= ",(" + 'cesg'+str(k) + ":Certifications_Subgroup {Name:'Certifications_Subgroup',Resume_Id:'"+ str(cv['CV_ID']) + "',sg:'"+str(k)+"'"
            second_dict_keys = list(cv['certifications'][k].keys())
            only_nodes = ['Course_Name','Certificate_Name','Course_Institution_Name']
            for j in second_dict_keys:
                if isinstance(cv['certifications'][k][j],date):
                    cv['certifications'][k][j] = cv['certifications'][k][j].strftime("%d %B %Y")
                if j not in only_nodes:
                    neo4j_create_statement+= "," + str(j) + ":'" + str(cv['certifications'][k][j]) + "'"
            neo4j_create_statement+= "})"
            certifications_elements.append('cesg'+str(k))
            for j in second_dict_keys:
                if j in only_nodes and cv['certifications'][k][j] != '':
                    neo4j_create_statement+= ",(cesg"+str(k)+ str(tagsreversed[j]) +":" + j + ":" + str(cv['certifications'][k][j]).replace(' ','_').replace('&','and').replace('.','').replace('-','_') + " {Name:'" + str(cv['certifications'][k][j]) + "'})"
                    if j == only_nodes[0]:
                       course_name_elements.append(str(cv['certifications'][k][j]).replace(' ','_').replace('&','and').replace('.','').replace('-','_'))
                       course_name_for_delete_list.append('cesg'+str(k)+ str(tagsreversed[j]))
                    elif j == only_nodes[1]:
                       certification_name_elements.append(str(cv['certifications'][k][j]).replace(' ','_').replace('&','and').replace('.','').replace('-','_'))
                       certification_name_for_delete_list.append('cesg'+str(k)+ str(tagsreversed[j]))
                    else:
                       course_institution_elements.append(str(cv['certifications'][k][j]).replace(' ','_').replace('&','and').replace('.','').replace('-','_'))
                       course_institution_for_delete_list.append('cesg'+str(k)+ str(tagsreversed[j]))

        neo4j_create_statement+= ",(`06`:Projects {Name:'Projects'})"
        dict_keys = list(cv['projects'].keys())
        for k in dict_keys:
            neo4j_create_statement+= ",(" + 'prsg'+str(k) + ":Projects_Subgroup {Name:'Projects_Subgroup',Resume_Id:'"+ str(cv['CV_ID']) + "',sg:'"+str(k)+"'"
            second_dict_keys = list(cv['projects'][k].keys())
            only_nodes = ['Project_Name','Project_Role','Project_Client']
            for j in second_dict_keys:
                if isinstance(cv['projects'][k][j],date):
                    cv['projects'][k][j] = cv['projects'][k][j].strftime("%d %B %Y")
                if j not in only_nodes:
                    neo4j_create_statement+= "," + str(j) + ":'" + str(cv['projects'][k][j]) +"'"
            neo4j_create_statement+= "})"
            projects_elements.append('prsg'+str(k))
            for j in second_dict_keys:
                if j in only_nodes and cv['projects'][k][j] != '':
                    neo4j_create_statement+= ",(prsg"+str(k)+ str(tagsreversed[j]) +":" + j + ":" + str(cv['projects'][k][j]).replace(' ','_').replace('&','and').replace('.','').replace('-','_') + " {Name:'" + str(cv['projects'][k][j]) + "'})"
                    if j == only_nodes[0]:
                        project_name_elements.append(str(cv['projects'][k][j]).replace(' ','_').replace('&','and').replace('.','').replace('-','_'))
                        project_name_for_delete_list.append('prsg'+str(k)+ str(tagsreversed[j]))
                    elif j == only_nodes[1]:
                        project_role_elements.append(str(cv['projects'][k][j]).replace(' ','_').replace('&','and').replace('.','').replace('-','_'))
                        project_role_for_delete_list.append('prsg'+str(k)+ str(tagsreversed[j]))
                    else:
                        project_client_elements.append(str(cv['projects'][k][j]).replace(' ','_').replace('&','and').replace('.','').replace('-','_'))
                        project_client_for_delete_list.append('prsg'+str(k)+ str(tagsreversed[j]))

        neo4j_create_statement+= ",(`07`:Awards {Name:'Awards'})"
        dict_keys = list(cv['awards'].keys())
        for k in dict_keys:
            neo4j_create_statement+= ",(" + 'awsg'+str(k) + ":Awards_Subgroup {Name:'Awards_Subgroup',Resume_Id:'"+ str(cv['CV_ID']) + "',sg:'"+str(k)+"'"
            second_dict_keys = list(cv['awards'][k].keys())
            only_nodes = ['Award_Title','Awarder']
            for j in second_dict_keys:
                if isinstance(cv['awards'][k][j],date):
                    cv['awards'][k][j] = cv['awards'][k][j].strftime("%d %B %Y")
                if j not in only_nodes:
                    neo4j_create_statement+= "," + str(j) + ":'" + str(cv['awards'][k][j]) + "'"
            neo4j_create_statement+= "})"
            awards_elements.append('awsg'+str(k))
            for j in second_dict_keys:
                if j in only_nodes and cv['awards'][k][j] != '':
                    neo4j_create_statement+= ",(awsg"+str(k)+ str(tagsreversed[j]) +":" + j + ":" + str(cv['awards'][k][j]).replace(' ','_').replace('&','and').replace('.','').replace('-','_') + " {Name:'" + str(cv['awards'][k][j]) + "'})"
                    if j == only_nodes[0]:
                        award_title_elements.append(str(cv['awards'][k][j]).replace(' ','_').replace('&','and').replace('.','').replace('-','_'))
                        award_title_for_delete_list.append('awsg'+str(k)+ str(tagsreversed[j]))
                    else:
                        awarder_elements.append(str(cv['awards'][k][j]).replace(' ','_').replace('&','and').replace('.','').replace('-','_'))
                        awarder_for_delete_list.append('awsg'+str(k)+ str(tagsreversed[j]))

        neo4j_create_statement+= ",(`08`:Publications {Name:'Publications'})"
        dict_keys = list(cv['publications'].keys())
        for k in dict_keys:
            neo4j_create_statement+= ",(" + 'pusg'+str(k) + ":Publications_Subgroup {Name:'Publications_Subgroup',Resume_Id:'"+ str(cv['CV_ID']) + "',sg:'"+str(k)+"'"
            second_dict_keys = list(cv['publications'][k].keys())
            only_nodes = ['Publication_Name','Publisher']
            for j in second_dict_keys:
                if isinstance(cv['publications'][k][j],date):
                    cv['publications'][k][j] = cv['publications'][k][j].strftime("%d %B %Y")
                if cv['publications'][k][j] not in only_nodes:
                    neo4j_create_statement+= "," + str(j) + ":'" + str(cv['publications'][k][j]) + "'"
            neo4j_create_statement+= "})"
            publications_elements.append('pusg'+str(k))
            for j in second_dict_keys and cv['publications'][k][j] != '':
                if cv['publications'][k][j] in only_nodes:
                    neo4j_create_statement+= ",(pusg"+str(k)+ str(tagsreversed[j]) +":" + j + ":" + str(cv['publications'][k][j]).replace(' ','_').replace('&','and').replace('.','').replace('-','_') + " {Name:'" + str(cv['publications'][k][j]) + "'})"
                    if j == only_nodes[0]:
                        publication_name_elements.append(str(cv['publication'][k][j]).replace(' ','_').replace('&','and').replace('.','').replace('-','_'))
                        publication_name_for_delete_list.append('pusg'+str(k)+ str(tagsreversed[j]))
                    else:
                        publisher_elements.append(str(cv['publication'][k][j]).replace(' ','_').replace('&','and').replace('.','').replace('-','_'))
                        publisher_for_delete_list.append('pusg'+str(k)+ str(tagsreversed[j]))

        neo4j_create_statement+= ",(`09`:References {Name:'References'})"
        dict_keys = list(cv['references'].keys())
        for k in dict_keys:
            neo4j_create_statement+= ",(" + 'resg'+str(k) + ":References_Subgroup {Name:'References_Subgroup',Resume_Id:'"+ str(cv['CV_ID']) + "',sg:'"+str(k)+"'"
            second_dict_keys = list(cv['references'][k].keys())
            only_nodes = []
            for j in second_dict_keys:
                if isinstance(cv['references'][k][j],date):
                    cv['references'][k][j] = cv['references'][k][j].strftime("%d %B %Y")
                if j not in only_nodes:
                    neo4j_create_statement+= "," + str(j) + ":'" + str(cv['references'][k][j]) +"'"
            neo4j_create_statement+= "})"
            references_elements.append('resg'+str(k))
            for j in second_dict_keys:
                if j in only_nodes and cv['references'][k][j] != '':
                    neo4j_create_statement+= ",(resg"+str(k)+ str(tagsreversed[j]) +":" + j + ":" + str(cv['references'][k][j]).replace(' ','_').replace('&','and').replace('.','').replace('-','_') + " {Name:'" + str(cv['references'][k][j]) + "'})"
                    references_elements.append("resg"+str(k)+ str(tagsreversed[j]))

        commands_elements.append(personal_elements)
        commands_elements.append(education_elements)
        commands_elements.append(experience_elements)
        commands_elements.append(certifications_elements)
        commands_elements.append(projects_elements)
        commands_elements.append(awards_elements)
        commands_elements.append(publications_elements)
        commands_elements.append(references_elements)

        for_delete_lists.append(skill_for_delete_list)
        for_delete_lists.append(institution_for_delete_list)
        for_delete_lists.append(education_title_for_delete_list)
        for_delete_lists.append(company_for_delete_list)
        for_delete_lists.append(job_for_delete_list)
        for_delete_lists.append(course_name_for_delete_list)
        for_delete_lists.append(certification_name_for_delete_list)
        for_delete_lists.append(course_institution_for_delete_list)
        for_delete_lists.append(project_name_for_delete_list)
        for_delete_lists.append(project_role_for_delete_list)
        for_delete_lists.append(project_client_for_delete_list)
        for_delete_lists.append(award_title_for_delete_list)
        for_delete_lists.append(awarder_for_delete_list)
        for_delete_lists.append(publication_name_for_delete_list)
        for_delete_lists.append(publisher_for_delete_list)

        inner_elements.append(skills_elements)
        inner_elements.append(institution_elements)
        inner_elements.append(education_title_elements)
        inner_elements.append(company_elements)
        inner_elements.append(job_title_elements)
        inner_elements.append(course_name_elements)
        inner_elements.append(certification_name_elements)
        inner_elements.append(course_institution_elements)
        inner_elements.append(project_name_elements)
        inner_elements.append(project_role_elements)
        inner_elements.append(project_client_elements)
        inner_elements.append(award_title_elements)
        inner_elements.append(awarder_elements)
        inner_elements.append(publication_name_elements)
        inner_elements.append(publisher_elements)

        return neo4j_create_statement,commands_elements,inner_elements,for_delete_lists

    def node_relation_builder(cv,neo4j_create_statement,commands_elements,inner_elements,for_delete_lists):
        all_skills_relation_statement_list = []
        cv_skill_relation_statement_list = []
        all_job_titles_relation_statement_list = []
        cv_job_title_relation_statement_list = []
        all_companys_relation_statement_list = []
        cv_company_relation_statement_list = []
        all_institutions_relation_statement_list = []
        cv_institution_relation_statement_list = []
        all_education_titles_relation_statement_list = []
        cv_education_title_relation_statement_list = []
        all_course_names_relation_statement_list = []
        cv_course_name_relation_statement_list = []
        all_certification_names_relation_statement_list = []
        cv_certification_name_relation_statement_list = []
        all_course_institutions_relation_statement_list = []
        cv_course_institution_relation_statement_list = []
        all_project_names_relation_statement_list = []
        cv_project_name_relation_statement_list = []
        all_project_roles_relation_statement_list = []
        cv_project_role_relation_statement_list = []
        all_project_clients_relation_statement_list = []
        cv_project_client_relation_statement_list = []
        all_award_titles_relation_statement_list = []
        cv_award_title_relation_statement_list = []
        all_awarders_relation_statement_list = []
        cv_awarder_relation_statement_list = []
        all_publication_names_relation_statement_list = []
        cv_publication_name_relation_statement_list = []
        all_publishers_relation_statement_list = []
        cv_publisher_relation_statement_list = []

        all_relation_statement_list = []
        cv_relation_statement_list = []

        neo4j_relation_statement = ",(`0`)-[:`HAS` ]->(`01`),(`0`)-[:`HAS` ]->(`02`),(`0`)-[:`HAS` ]->(`03`),(`0`)-[:`HAS` ]->(`04`),(`0`)-[:`HAS` ]->(`05`),(`0`)-[:`HAS` ]->(`06`),(`0`)-[:`HAS` ]->(`07`),(`0`)-[:`HAS` ]->(`08`),(`0`)-[:`HAS` ]->(`09`)"
        for j in range(len(commands_elements)):
            if j == 0:
                for z in range(len(commands_elements[j])):
                    neo4j_relation_statement+= ",(`01`)-[:`Contact` ]->(`"+commands_elements[j][z]+"`)"
            if j == 1:
                for z in range(len(commands_elements[j])):
                    if 'sg' in commands_elements[j][z]:
                        subgroup = commands_elements[j][z]
                        neo4j_relation_statement+= ",(`02`)-[:`EDUCATION_SUBGROUP` ]->("+subgroup+")"

            if j == 2:
                for z in range(len(commands_elements[j])):
                    if 'sg' in commands_elements[j][z]:
                        subgroup = commands_elements[j][z]
                        neo4j_relation_statement+= ",(`03`)-[:`EXPERIENCE_SUBGROUP` ]->("+subgroup+")"

            if j == 3:
                for z in range(len(commands_elements[j])):
                    if 'sg' in commands_elements[j][z]:
                        subgroup = commands_elements[j][z]
                        neo4j_relation_statement+= ",(`05`)-[:`CERTIFICATIONS_SUBGROUP` ]->("+subgroup+")"

            if j == 4:
                for z in range(len(commands_elements[j])):
                    if 'sg' in commands_elements[j][z]:
                        subgroup = commands_elements[j][z]
                        neo4j_relation_statement+= ",(`06`)-[:`PROJECTS_SUBGROUP` ]->("+subgroup+")"

            if j == 5:
                for z in range(len(commands_elements[j])):
                    if 'sg' in commands_elements[j][z]:
                        subgroup = commands_elements[j][z]
                        neo4j_relation_statement+= ",(`07`)-[:`AWARDS_SUBGROUP` ]->("+subgroup+")"

            if j == 6:
                for z in range(len(commands_elements[j])):
                    if 'sg' in commands_elements[j][z]:
                        subgroup = commands_elements[j][z]
                        neo4j_relation_statement+= ",(`08`)-[:`PUBLICATIONS_SUBGROUP` ]->("+subgroup+")"

            if j == 7:
                for z in range(len(commands_elements[j])):
                    if 'sg' in commands_elements[j][z]:
                        subgroup = commands_elements[j][z]
                        neo4j_relation_statement+= ",(`00`)-[:`REFERENCES_SUBGROUP` ]->("+subgroup+")"

        for i in range(len(inner_elements[0])):
            LOGGER.info(inner_elements[0][i])
            check_query = "MATCH (as:All_Skills)-[:CONTAIN_SKILL]->(z:"+inner_elements[0][i]+") RETURN as,z"
            if len(execute_match(check_query)) == 0:
                LOGGER.info(cv['CV_ID'])
                all_skills_relation_statement = "MATCH (as:All_Skills),(z:"+inner_elements[0][i]+") MERGE (as)-[:CONTAIN_SKILL]->(z)"
                all_skills_relation_statement_list.append(all_skills_relation_statement)
                cv_skill_relation_statement= "MATCH (r:Skills) where r.Resume_Id='"+ str(cv['CV_ID']) + "' MATCH (z:"+inner_elements[0][i]+") MERGE (r)-[:HAS_SKILL]->(z)"
                cv_skill_relation_statement_list.append(cv_skill_relation_statement)
            else:
                neo4j_relation_statement+= " DELETE "+for_delete_lists[0][i]
                cv_skill_relation_statement = "MATCH (r:Skills) where r.Resume_Id='"+ str(cv['CV_ID']) + "' MATCH (z:"+inner_elements[0][i]+") MERGE (r)-[:HAS_SKILL]->(z)"
                cv_skill_relation_statement_list.append(cv_skill_relation_statement)

        for i in range(len(inner_elements[4])):
            LOGGER.info(inner_elements[4][i])
            check_query = "MATCH (as:All_Job_Titles)-[:CONTAIN_JOB_TITLE]->(z:"+inner_elements[4][i]+") RETURN as,z"
            if len(execute_match(check_query)) == 0:
                LOGGER.info(cv['CV_ID'])
                all_job_titles_relation_statement = "MATCH (as:All_Job_Titles),(z:"+inner_elements[4][i]+") MERGE (as)-[:CONTAIN_JOB_TITLE]->(z)"
                all_job_titles_relation_statement_list.append(all_job_titles_relation_statement)
                cv_job_title_relation_statement= "MATCH (r:Experience_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[4][i][4])+"' MATCH (z:"+inner_elements[4][i]+") MERGE (r)-[:HAS_JOB_TITLE]->(z)"
                cv_job_title_relation_statement_list.append(cv_job_title_relation_statement)
            else:
                neo4j_relation_statement+= " DELETE "+for_delete_lists[4][i]
                cv_job_title_relation_statement= "MATCH (r:Experience_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[4][i][4])+"' MATCH (z:"+inner_elements[4][i]+") MERGE (r)-[:HAS_JOB_TITLE]->(z)"
                cv_job_title_relation_statement_list.append(cv_job_title_relation_statement)

        for i in range(len(inner_elements[3])):
            LOGGER.info(inner_elements[3][i])
            check_query = "MATCH (as:All_Company_Names)-[:CONTAIN_COMPANY_NAME]->(z:"+inner_elements[3][i]+") RETURN as,z"
            if len(execute_match(check_query)) == 0:
                LOGGER.info(cv['CV_ID'])
                all_companys_relation_statement = "MATCH (as:All_Company_Names),(z:"+inner_elements[3][i]+") MERGE (as)-[:CONTAIN_COMPANY_NAME]->(z)"
                all_companys_relation_statement_list.append(all_companys_relation_statement)
                cv_company_relation_statement= "MATCH (r:Experience_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[3][i][4])+"' MATCH (z:"+inner_elements[3][i]+") MERGE (r)-[:HAS_COMPANY_NAME]->(z)"
                cv_company_relation_statement_list.append(cv_company_relation_statement)
            else:
                neo4j_relation_statement+= " DELETE "+for_delete_lists[3][i]
                cv_company_relation_statement= "MATCH (r:Experience_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[3][i][4])+"' MATCH (z:"+inner_elements[3][i]+") MERGE (r)-[:HAS_COMPANY_NAME]->(z)"
                cv_company_relation_statement_list.append(cv_company_relation_statement)

        for i in range(len(inner_elements[1])):
            LOGGER.info(inner_elements[1][i])
            check_query = "MATCH (as:All_Institution_Names)-[:CONTAIN_INSTITUTION_NAME]->(z:"+inner_elements[1][i]+") RETURN as,z"
            if len(execute_match(check_query)) == 0:
                LOGGER.info(cv['CV_ID'])
                all_institutions_relation_statement = "MATCH (as:All_Institution_Names),(z:"+inner_elements[1][i]+") MERGE (as)-[:CONTAIN_INSTITUTION_NAME]->(z)"
                all_institutions_relation_statement_list.append(all_institutions_relation_statement)
                cv_institution_relation_statement= "MATCH (r:Education_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[1][i][4])+"' MATCH (z:"+inner_elements[1][i]+") MERGE (r)-[:HAS_INSTITUTION_NAME]->(z)"
                cv_institution_relation_statement_list.append(cv_institution_relation_statement)
            else:
                neo4j_relation_statement+= " DELETE "+for_delete_lists[1][i]
                cv_institution_relation_statement= "MATCH (r:Education_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[1][i][4])+"' MATCH (z:"+inner_elements[1][i]+") MERGE (r)-[:HAS_INSTITUTION_NAME]->(z)"
                cv_institution_relation_statement_list.append(cv_institution_relation_statement)

        for i in range(len(inner_elements[2])):
            LOGGER.info(inner_elements[2][i])
            check_query = "MATCH (as:All_Education_Titles)-[:CONTAIN_EDUCATION_TITLE]->(z:"+inner_elements[2][i]+") RETURN as,z"
            if len(execute_match(check_query)) == 0:
                LOGGER.info(cv['CV_ID'])
                all_education_titles_relation_statement = "MATCH (as:All_Education_Titles),(z:"+inner_elements[2][i]+") MERGE (as)-[:CONTAIN_EDUCATION_TITLE]->(z)"
                all_education_titles_relation_statement_list.append(all_education_titles_relation_statement)
                cv_education_title_relation_statement= "MATCH (r:Education_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[2][i][4])+"' MATCH (z:"+inner_elements[2][i]+") MERGE (r)-[:HAS_EDUCATION_TITLE]->(z)"
                cv_education_title_relation_statement_list.append(cv_education_title_relation_statement)
            else:
                neo4j_relation_statement+= " DELETE "+for_delete_lists[2][i]
                cv_education_title_relation_statement= "MATCH (r:Education_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[2][i][4])+"' MATCH (z:"+inner_elements[2][i]+") MERGE (r)-[:HAS_EDUCATION_TITLE]->(z)"
                cv_education_title_relation_statement_list.append(cv_education_title_relation_statement)

        for i in range(len(inner_elements[5])):
            LOGGER.info(inner_elements[5][i])
            check_query = "MATCH (as:All_Course_Names)-[:CONTAIN_COURSE_NAME]->(z:"+inner_elements[5][i]+") RETURN as,z"
            if len(execute_match(check_query)) == 0:
                LOGGER.info(cv['CV_ID'])
                all_course_names_relation_statement = "MATCH (as:All_Course_Names),(z:"+inner_elements[5][i]+") MERGE (as)-[:CONTAIN_COURSE_NAME]->(z)"
                all_course_names_relation_statement_list.append(all_course_names_relation_statement)
                cv_course_name_relation_statement= "MATCH (r:Certifications_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[5][i][4])+"' MATCH (z:"+inner_elements[5][i]+") MERGE (r)-[:HAS_COURSE_NAME]->(z)"
                cv_course_name_relation_statement_list.append(cv_course_name_relation_statement)
            else:
                neo4j_relation_statement+= " DELETE "+for_delete_lists[5][i]
                cv_course_name_relation_statement= "MATCH (r:Certifications_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[5][i][4])+"' MATCH (z:"+inner_elements[5][i]+") MERGE (r)-[:HAS_COURSE_NAME]->(z)"
                cv_course_name_relation_statement_list.append(cv_course_name_relation_statement)

        for i in range(len(inner_elements[6])):
            LOGGER.info(inner_elements[6][i])
            check_query = "MATCH (as:All_Certification_Names)-[:CONTAIN_CERTIFICATION_NAME]->(z:"+inner_elements[6][i]+") RETURN as,z"
            if len(execute_match(check_query)) == 0:
                LOGGER.info(cv['CV_ID'])
                all_certification_names_relation_statement = "MATCH (as:All_Certification_Names),(z:"+inner_elements[6][i]+") MERGE (as)-[:CONTAIN_CERTIFICATION_NAME]->(z)"
                all_certification_names_relation_statement_list.append(all_certification_names_relation_statement)
                cv_certification_name_relation_statement= "MATCH (r:Certifications_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[6][i][4])+"' MATCH (z:"+inner_elements[6][i]+") MERGE (r)-[:HAS_CERTIFICATION_NAME]->(z)"
                cv_certification_name_relation_statement_list.append(cv_certification_name_relation_statement)
            else:
                neo4j_relation_statement+= " DELETE "+for_delete_lists[6][i]
                cv_certification_name_relation_statement= "MATCH (r:Certifications_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[6][i][4])+"' MATCH (z:"+inner_elements[6][i]+") MERGE (r)-[:HAS_CERTIFICATION_NAME]->(z)"
                cv_certification_name_relation_statement_list.append(cv_certification_name_relation_statement)

        for i in range(len(inner_elements[7])):
            LOGGER.info(inner_elements[7][i])
            check_query = "MATCH (as:All_Course_Institutions)-[:CONTAIN_COURSE_INSTITUTION]->(z:"+inner_elements[7][i]+") RETURN as,z"
            if len(execute_match(check_query)) == 0:
                LOGGER.info(cv['CV_ID'])
                all_course_institutions_relation_statement = "MATCH (as:All_Course_Institutions),(z:"+inner_elements[7][i]+") MERGE (as)-[:CONTAIN_COURSE_INSTITUTION]->(z)"
                all_course_institutions_relation_statement_list.append(all_course_institutions_relation_statement)
                cv_course_institution_relation_statement= "MATCH (r:Certifications_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[7][i][4])+"' MATCH (z:"+inner_elements[7][i]+") MERGE (r)-[:HAS_COURSE_INSTITUTION]->(z)"
                cv_course_institution_relation_statement_list.append(cv_course_institution_relation_statement)
            else:
                neo4j_relation_statement+= " DELETE "+for_delete_lists[7][i]
                cv_course_institution_relation_statement= "MATCH (r:Certifications_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[7][i][4])+"' MATCH (z:"+inner_elements[7][i]+") MERGE (r)-[:HAS_COURSE_INSTITUTION]->(z)"
                cv_course_institution_relation_statement_list.append(cv_course_institution_relation_statement)

        for i in range(len(inner_elements[8])):
            LOGGER.info(inner_elements[8][i])
            check_query = "MATCH (as:All_Project_Names)-[:CONTAIN_PROJECT_NAME]->(z:"+inner_elements[8][i]+") RETURN as,z"
            if len(execute_match(check_query)) == 0:
                LOGGER.info(cv['CV_ID'])
                all_project_names_relation_statement = "MATCH (as:All_Project_Names),(z:"+inner_elements[8][i]+") MERGE (as)-[:CONTAIN_PROJECT_NAME]->(z)"
                all_project_names_relation_statement_list.append(all_project_names_relation_statement)
                cv_project_name_relation_statement= "MATCH (r:Projects_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[8][i][4])+"' MATCH (z:"+inner_elements[8][i]+") MERGE (r)-[:HAS_PROJECT_NAME]->(z)"
                cv_project_name_relation_statement_list.append(cv_project_name_relation_statement)
            else:
                neo4j_relation_statement+= " DELETE "+for_delete_lists[8][i]
                cv_project_name_relation_statement= "MATCH (r:Projects_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[8][i][4])+"' MATCH (z:"+inner_elements[8][i]+") MERGE (r)-[:HAS_PROJECT_NAME]->(z)"
                cv_project_name_relation_statement_list.append(cv_project_name_relation_statement)

        for i in range(len(inner_elements[9])):
            LOGGER.info(inner_elements[9][i])
            check_query = "MATCH (as:All_Project_Roles)-[:CONTAIN_PROJECT_ROLE]->(z:"+inner_elements[9][i]+") RETURN as,z"
            if len(execute_match(check_query)) == 0:
                LOGGER.info(cv['CV_ID'])
                all_project_roles_relation_statement = "MATCH (as:All_Project_Roles),(z:"+inner_elements[9][i]+") MERGE (as)-[:CONTAIN_PROJECT_ROLE]->(z)"
                all_project_roles_relation_statement_list.append(all_project_roles_relation_statement)
                cv_project_role_relation_statement= "MATCH (r:Projects_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[9][i][4])+"' MATCH (z:"+inner_elements[9][i]+") MERGE (r)-[:HAS_PROJECT_ROLE]->(z)"
                cv_project_role_relation_statement_list.append(cv_project_role_relation_statement)
            else:
                neo4j_relation_statement+= " DELETE "+for_delete_lists[9][i]
                cv_project_role_relation_statement= "MATCH (r:Projects_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[9][i][4])+"' MATCH (z:"+inner_elements[9][i]+") MERGE (r)-[:HAS_PROJECT_ROLE]->(z)"
                cv_project_role_relation_statement_list.append(cv_project_role_relation_statement)

        for i in range(len(inner_elements[10])):
            LOGGER.info(inner_elements[10][i])
            check_query = "MATCH (as:All_Project_Clients)-[:CONTAIN_PROJECT_CLIENT]->(z:"+inner_elements[10][i]+") RETURN as,z"
            if len(execute_match(check_query)) == 0:
                LOGGER.info(cv['CV_ID'])
                all_project_clients_relation_statement = "MATCH (as:All_Project_Clients),(z:"+inner_elements[10][i]+") MERGE (as)-[:CONTAIN_PROJECT_CLIENT]->(z)"
                all_project_clients_relation_statement_list.append(all_project_clients_relation_statement)
                cv_project_client_relation_statement= "MATCH (r:Projects_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[10][i][4])+"' MATCH (z:"+inner_elements[10][i]+") MERGE (r)-[:HAS_PROJECT_CLIENT]->(z)"
                cv_project_client_relation_statement_list.append(cv_project_client_relation_statement)
            else:
                neo4j_relation_statement+= " DELETE "+for_delete_lists[10][i]
                cv_project_client_relation_statement= "MATCH (r:Projects_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[10][i][4])+"' MATCH (z:"+inner_elements[10][i]+") MERGE (r)-[:HAS_PROJECT_CLIENT]->(z)"
                cv_project_client_relation_statement_list.append(cv_project_client_relation_statement)

        for i in range(len(inner_elements[11])):
            LOGGER.info(inner_elements[11][i])
            check_query = "MATCH (as:All_Award_Titles)-[:CONTAIN_AWARD_TITLE]->(z:"+inner_elements[11][i]+") RETURN as,z"
            if len(execute_match(check_query)) == 0:
                LOGGER.info(cv['CV_ID'])
                all_award_titles_relation_statement = "MATCH (as:All_Award_Titles),(z:"+inner_elements[11][i]+") MERGE (as)-[:CONTAIN_AWARD_TITLE]->(z)"
                all_award_titles_relation_statement_list.append(all_award_titles_relation_statement)
                cv_award_title_relation_statement= "MATCH (r:Awards_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[11][i][4])+"' MATCH (z:"+inner_elements[11][i]+") MERGE (r)-[:HAS_AWARD_TITLE]->(z)"
                cv_award_title_relation_statement_list.append(cv_award_title_relation_statement)
            else:
                neo4j_relation_statement+= " DELETE "+for_delete_lists[11][i]
                cv_award_title_relation_statement= "MATCH (r:Awards_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[11][i][4])+"' MATCH (z:"+inner_elements[11][i]+") MERGE (r)-[:HAS_AWARD_TITLE]->(z)"
                cv_award_title_relation_statement_list.append(cv_award_title_relation_statement)

        for i in range(len(inner_elements[12])):
            LOGGER.info(inner_elements[12][i])
            check_query = "MATCH (as:All_Awarders)-[:CONTAIN_AWARDER]->(z:"+inner_elements[12][i]+") RETURN as,z"
            if len(execute_match(check_query)) == 0:
                LOGGER.info(cv['CV_ID'])
                all_awarders_relation_statement = "MATCH (as:All_Awarders),(z:"+inner_elements[12][i]+") MERGE (as)-[:CONTAIN_AWARDER]->(z)"
                all_awarders_relation_statement_list.append(all_awarders_relation_statement)
                cv_awarder_relation_statement= "MATCH (r:Awards_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[12][i][4])+"' MATCH (z:"+inner_elements[12][i]+") MERGE (r)-[:HAS_AWARDER]->(z)"
                cv_awarder_relation_statement_list.append(cv_awarder_relation_statement)
            else:
                neo4j_relation_statement+= " DELETE "+for_delete_lists[12][i]
                cv_awarder_relation_statement= "MATCH (r:Awards_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[12][i][4])+"' MATCH (z:"+inner_elements[12][i]+") MERGE (r)-[:HAS_AWARDER]->(z)"
                cv_awarder_relation_statement_list.append(cv_awarder_relation_statement)

        for i in range(len(inner_elements[13])):
            LOGGER.info(inner_elements[13][i])
            check_query = "MATCH (as:All_Publication_Names)-[:CONTAIN_PUBLICATION_NAME]->(z:"+inner_elements[13][i]+") RETURN as,z"
            if len(execute_match(check_query)) == 0:
                LOGGER.info(cv['CV_ID'])
                all_publication_names_relation_statement = "MATCH (as:All_Publication_Names),(z:"+inner_elements[13][i]+") MERGE (as)-[:CONTAIN_PUBLICATION_NAME]->(z)"
                all_publication_names_relation_statement_list.append(all_publication_names_relation_statement)
                cv_publication_name_relation_statement= "MATCH (r:Publications_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[13][i][4])+"' MATCH (z:"+inner_elements[13][i]+") MERGE (r)-[:HAS_PUBLICATION_NAME]->(z)"
                cv_publication_name_relation_statement_list.append(cv_publication_name_relation_statement)
            else:
                neo4j_relation_statement+= " DELETE "+for_delete_lists[13][i]
                cv_publication_name_relation_statement= "MATCH (r:Publications_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[13][i][4])+"' MATCH (z:"+inner_elements[13][i]+") MERGE (r)-[:HAS_PUBLICATION_NAME]->(z)"
                cv_publication_name_relation_statement_list.append(cv_publication_name_relation_statement)

        for i in range(len(inner_elements[14])):
            LOGGER.info(inner_elements[14][i])
            check_query = "MATCH (as:All_Publishers)-[:CONTAIN_PUBLISHER]->(z:"+inner_elements[14][i]+") RETURN as,z"
            if len(execute_match(check_query)) == 0:
                LOGGER.info(cv['CV_ID'])
                all_publishers_relation_statement = "MATCH (as:All_Publishers),(z:"+inner_elements[14][i]+") MERGE (as)-[:CONTAIN_PUBLISHER]->(z)"
                all_publishers_relation_statement_list.append(all_publishers_relation_statement)
                cv_publisher_relation_statement= "MATCH (r:Publications_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[14][i][4])+"' MATCH (z:"+inner_elements[14][i]+") MERGE (r)-[:HAS_PUBLISHER]->(z)"
                cv_publisher_relation_statement_list.append(cv_publisher_relation_statement)
            else:
                neo4j_relation_statement+= " DELETE "+for_delete_lists[14][i]
                cv_publisher_relation_statement= "MATCH (r:Publications_Subgroup) where r.Resume_Id='"+ str(cv['CV_ID']) + "' AND r.sg='"+str(for_delete_lists[14][i][4])+"' MATCH (z:"+inner_elements[14][i]+") MERGE (r)-[:HAS_PUBLISHER]->(z)"
                cv_publisher_relation_statement_list.append(cv_publisher_relation_statement)

        full = neo4j_create_statement + neo4j_relation_statement
        all_relation_statement_list = [all_skills_relation_statement_list, all_institutions_relation_statement_list,
                                       all_education_titles_relation_statement_list, all_companys_relation_statement_list,
                                       all_job_titles_relation_statement_list, all_course_names_relation_statement_list,
                                       all_certification_names_relation_statement_list, all_course_institutions_relation_statement_list,
                                       all_project_names_relation_statement_list, all_project_roles_relation_statement_list,
                                       all_project_clients_relation_statement_list, all_award_titles_relation_statement_list,
                                       all_awarders_relation_statement_list, all_publication_names_relation_statement_list,
                                       all_publishers_relation_statement_list]

        cv_relation_statement_list = [cv_skill_relation_statement_list, cv_institution_relation_statement_list,
                                      cv_education_title_relation_statement_list, cv_company_relation_statement_list,
                                      cv_job_title_relation_statement_list, cv_course_name_relation_statement_list,
                                      cv_certification_name_relation_statement_list, cv_course_institution_relation_statement_list,
                                      cv_project_name_relation_statement_list, cv_project_role_relation_statement_list,
                                      cv_project_client_relation_statement_list, cv_award_title_relation_statement_list,
                                      cv_awarder_relation_statement_list, cv_publication_name_relation_statement_list,
                                      cv_publisher_relation_statement_list]

        return full,all_relation_statement_list,cv_relation_statement_list

    def execute_match(execution_commands):
        from neo4j import GraphDatabase
        data_base_connection = GraphDatabase.driver(uri = "bolt://neo4j:7687", auth=("neo4j", "neo4j"))
        session = data_base_connection.session()
        #     for i in execution_commands:
        i = execution_commands
        Nodes = session.run(i)
        results = [record for record in Nodes.data()]

        # Now you can access the Node using the key 'n' (defined in the return statement):
        a = results
        return a

    def execute_actions(**kwargs):
        from neo4j import GraphDatabase
        import json
        for i in range(len(listvalues)):
            tagsreversed[listvalues[i]] = listkeys[i]
        file_data = None
        cv_id=str(kwargs['dag_run'].conf['file_id'])
        file_name = '/home/spring/uploaded-cv/'+cv_id+'.json'
        with open(file_name, encoding='utf-8') as f:
            file_data = json.load(f)
        file_data['CV_ID']=cv_id
        data_base_connection = GraphDatabase.driver(uri = "bolt://neo4j:7687", auth=("neo4j", "neo4j"))
        session = data_base_connection.session()
        ncs,ce,ie,fdl = node_create_builder(file_data)
        LOGGER.info(file_data['CV_ID'])
        full,arst,crst=node_relation_builder(file_data,ncs,ce,ie,fdl)
        session.run(full)

        for z in range(15):
            for i in arst[z]:
                session.run(i)
        for z in range(15):
            for i in crst[z]:
                session.run(i)

        return 'Done'

    neo4j = PythonOperator(
        task_id='neo4j',
        python_callable=execute_actions,
        provide_context=True,
        dag=dag,
    )

    def elastic_ingestion(**kwargs):
        import json
        from elasticsearch import Elasticsearch
        es = Elasticsearch([{'host': 'elastic', 'port': 9200}])
        cv_id=str(kwargs['dag_run'].conf['file_id'])
        file_name = '/home/spring/uploaded-cv/'+cv_id+'.json'
        with open(file_name, encoding='utf-8') as f:
            file_data = json.load(f)
        file_data['CV_ID']=cv_id
        LOGGER.info(file_data)
        es.index(index='tweets', doc_type='users', id=cv_id, body=file_data)

    elastic = PythonOperator(
        task_id='elastic',
        python_callable=elastic_ingestion,
        provide_context=True,
        dag=dag,
    )


    prerequisite >> t1
    t1 >> t2
    t2 >> elastic
    t2 >> neo4j