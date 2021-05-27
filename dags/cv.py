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

    prerequisite = BashOperator(
        task_id='prerequisite',
        bash_command='pip install neo4j'
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

        skills_elements = []
        institution_elements = []
        education_title_elements = []
        job_title_elements = []
        company_elements = []

        neo4j_create_statement = "create (`0`:Resume {Resume_Id:'" + str(cv['CV_ID']) + "'}),(`01`:Personal_Information {Name:'Personal Information'"
        dict_keys = list(cv['personal'].keys())

        for k in dict_keys:
            if isinstance(cv['personal'][k],list):
                try:
                    cv['personal'][k] = cv['personal'][k][0]
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
            if int(k) == 0:
                continue
            neo4j_create_statement+= ",(" + 'edsg'+str(k) + ":Education_Entity {Name:'Education_Entity'"
            second_dict_keys = list(cv['education'][k].keys())
            only_nodes = ['Education_Institution_Name','Education_Title']
            for j in second_dict_keys:
                if isinstance(cv['education'][k][j],date):
                    cv['education'][k][j] = cv['education'][k][j]
                if j not in only_nodes:
                    neo4j_create_statement+= "," + str(j) + ":'" + str(cv['education'][k][j]) + "'"
            neo4j_create_statement+= "})"
            education_elements.append('edsg'+str(k))
            for j in second_dict_keys:
                if j in only_nodes:
                    neo4j_create_statement+= ",(edsg"+str(k)+ str(tagsreversed[j]) +":" + j + ":" + str(cv['education'][k][j]).replace(' ','_') +" {Name:'" + str(cv['education'][k][j]) + "'})"
                    education_elements.append('edsg'+str(k)+ str(tagsreversed[j]))
        neo4j_create_statement+= ",(`03`:Experience {Name:'Experience',Resume_Id:'"+ str(cv['CV_ID']) + "'})"
        dict_keys = list(cv['experience'].keys())
        for k in dict_keys:
            if k == 0:
                continue
            neo4j_create_statement+= ",(" + 'exsg'+str(k) + ":Experience_Entity {Name:'Experience_Entity'"
            second_dict_keys = list(cv['experience'][k].keys())
            only_nodes = ['Company_Name','Job_Title']
            for j in second_dict_keys:
                if isinstance(cv['experience'][k][j],date):
                    cv['experience'][k][j] = cv['experience'][k][j]
                if j not in only_nodes:
                    neo4j_create_statement+= "," + str(j) + ":'" + str(cv['experience'][k][j]) + "'"
            neo4j_create_statement+= ",Resume_Id:'"+ str(cv['CV_ID']) + "'})"
            experience_elements.append('exsg'+str(k))
            for j in second_dict_keys:
                if j in only_nodes:
                    neo4j_create_statement+= ",(exsg"+str(k)+ str(tagsreversed[j]) +":" + j + ":" + str(cv['experience'][k][j]).replace(' ','_') + " {Name:'" + str(cv['experience'][k][j]) + "'})"
                    experience_elements.append('exsg'+str(k)+ str(tagsreversed[j]))
                    if j == only_nodes[0]:
                        company_elements.append(str(cv['experience'][k][j]).replace(' ','_'))
                    else:
                        job_title_elements.append(str(cv['experience'][k][j]).replace(' ','_'))

        neo4j_create_statement+= ",(`04`:Skills {Name:'Skills',Resume_Id:'"+ str(cv['CV_ID']) + "'})"
        dict_keys = list(cv['skills'])
        kk=0
        for k in dict_keys:
            neo4j_create_statement+= ",("+ 'skill'+str(kk)+':'+ str(cv['skills'][kk]).replace(' ','_') + " {Name:'" + str(cv['skills'][kk]) + "'})"
            skills_elements.append(str(cv['skills'][kk]).replace(' ','_'))
            kk+=1

        neo4j_create_statement+= ",(`05`:Certifications {Name:'Certifications'})"
        dict_keys = list(cv['certifications'].keys())
        for k in dict_keys:
            if k == 0:
                continue
            neo4j_create_statement+= ",(" + 'cesg'+str(k) + ":Certifications_Entity {Name:'Certifications_Entity'"
            second_dict_keys = list(cv['certifications'][k].keys())
            only_nodes = ['Course_Name','Certificate_Name','Course_Institution_Name']
            for j in second_dict_keys:
                if isinstance(cv['certifications'][k][j],date):
                    cv['certifications'][k][j] = cv['certifications'][k][j]
                if j not in only_nodes:
                    neo4j_create_statement+= "," + str(j) + ":'" + str(cv['certifications'][k][j]) + "'"
            neo4j_create_statement+= "})"
            certifications_elements.append('cesg'+str(k))
            for j in second_dict_keys:
                if j in only_nodes:
                    neo4j_create_statement+= ",(cesg"+str(k)+ str(tagsreversed[j]) +":" + j + ":" + str(cv['certifications'][k][j]).replace(' ','_') + " {Name:'" + str(cv['certifications'][k][j]) + "'})"
                    certifications_elements.append("cesg"+str(k)+ str(tagsreversed[j]))

        neo4j_create_statement+= ",(`06`:Projects {Name:'Projects'})"
        dict_keys = list(cv['projects'].keys())
        for k in dict_keys:
            if k == 0:
                continue
            neo4j_create_statement+= ",(" + 'prsg'+str(k) + ":Projects_Entity {Name:'Projects_Entity'"
            second_dict_keys = list(cv['projects'][k].keys())
            only_nodes = ['Project_Name','Project_Role','Project_Client']
            for j in second_dict_keys:
                if isinstance(cv['projects'][k][j],date):
                    cv['projects'][k][j] = cv['projects'][k][j]
                if j not in only_nodes:
                    neo4j_create_statement+= "," + str(j) + ":'" + str(cv['projects'][k][j]) +"'"
            neo4j_create_statement+= "})"
            projects_elements.append('prsg'+str(k))
            for j in second_dict_keys:
                if j in only_nodes:
                    neo4j_create_statement+= ",(prsg"+str(k)+ str(tagsreversed[j]) +":" + j + ":" + str(cv['projects'][k][j]).replace(' ','_') + " {Name:'" + str(cv['projects'][k][j]) + "'})"
                    projects_elements.append("prsg"+str(k)+ str(tagsreversed[j]))

        neo4j_create_statement+= ",(`07`:Awards {Name:'Awards'})"
        dict_keys = list(cv['awards'].keys())
        for k in dict_keys:
            if k == 0:
                continue
            neo4j_create_statement+= ",(" + 'awsg'+str(k) + ":Awards_Entity {Name:'Awards_Entity'"
            second_dict_keys = list(cv['awards'][k].keys())
            only_nodes = ['Award_Title','Awarder']
            for j in second_dict_keys:
                if isinstance(cv['awards'][k][j],date):
                    cv['awards'][k][j] = cv['awards'][k][j]
                if j not in only_nodes:
                    neo4j_create_statement+= "," + str(j) + ":'" + str(cv['awards'][k][j]) + "'"
            neo4j_create_statement+= "})"
            awards_elements.append('awsg'+str(k))
            for j in second_dict_keys:
                if j in only_nodes:
                    neo4j_create_statement+= ",(awsg"+str(k)+ str(tagsreversed[j]) +":" + j + ":" + str(cv['awards'][k][j]).replace(' ','_') + " {Name:'" + str(cv['awards'][k][j]) + "'})"
                    awards_elements.append("awsg"+str(k)+ str(tagsreversed[j]))

        neo4j_create_statement+= ",(`08`:Publications {Name:'Publications'})"
        dict_keys = list(cv['publications'].keys())
        for k in dict_keys:
            if k == 0:
                continue
            neo4j_create_statement+= ",(" + 'pusg'+str(k) + ":Publications_Entity {Name:'Publications_Entity'"
            second_dict_keys = list(cv['publications'][k].keys())
            only_nodes = ['Publication_Name','Publisher']
            for j in second_dict_keys:
                if isinstance(cv['publications'][k][j],date):
                    cv['publications'][k][j] = cv['publications'][k][j]
                if cv['publications'][k][j] not in only_nodes:
                    neo4j_create_statement+= "," + str(j) + ":'" + str(cv['publications'][k][j]) + "'"
            neo4j_create_statement+= "})"
            publications_elements.append('pusg'+str(k))
            for j in second_dict_keys:
                if cv['publications'][k][j] in only_nodes:
                    neo4j_create_statement+= ",(pusg"+str(k)+ str(tagsreversed[j]) +":" + j + ":" + str(cv['publications'][k][j]).replace(' ','_') + " {Name:'" + str(cv['publications'][k][j]) + "'})"
                    publications_elements.append("pusg"+str(k)+ str(tagsreversed[j]))

        neo4j_create_statement+= ",(`09`:References {Name:'References'})"
        dict_keys = list(cv['references'].keys())
        for k in dict_keys:
            if k == 0:
                continue
            neo4j_create_statement+= ",(" + 'resg'+str(k) + ":References_Entity {Name:'References_Entity'"
            second_dict_keys = list(cv['references'][k].keys())
            only_nodes = []
            for j in second_dict_keys:
                if isinstance(cv['references'][k][j],date):
                    cv['references'][k][j] = cv['references'][k][j]
                if j not in only_nodes:
                    neo4j_create_statement+= "," + str(j) + ":'" + str(cv['references'][k][j]) +"'"
            neo4j_create_statement+= "})"
            references_elements.append('resg'+str(k))
            for j in second_dict_keys:
                if j in only_nodes:
                    neo4j_create_statement+= ",(resg"+str(k)+ str(tagsreversed[j]) +":" + j + ":" + str(cv['references'][k][j]).replace(' ','_') + " {Name:'" + str(cv['references'][k][j]) + "'})"
                    references_elements.append("resg"+str(k)+ str(tagsreversed[j]))

        commands_elements.append(personal_elements)
        commands_elements.append(education_elements)
        commands_elements.append(experience_elements)
        commands_elements.append(certifications_elements)
        commands_elements.append(projects_elements)
        commands_elements.append(awards_elements)
        commands_elements.append(publications_elements)
        commands_elements.append(references_elements)
        return neo4j_create_statement,commands_elements,skills_elements,institution_elements,education_title_elements,company_elements,job_title_elements
    def node_relation_builder(cv,neo4j_create_statement,commands_elements,skills_elements,job_title_elements):
        all_skills_relation_statement_list = []
        cv_skill_relation_statement_list = []
        all_job_title_relation_statement_list = []
        cv_job_title_relation_statement_list = []
        neo4j_relation_statement = ",(`0`)-[:`HAS` ]->(`01`),(`0`)-[:`HAS` ]->(`02`),(`0`)-[:`HAS` ]->(`03`),(`0`)-[:`HAS` ]->(`04`),(`0`)-[:`HAS` ]->(`05`),(`0`)-[:`HAS` ]->(`06`),(`0`)-[:`HAS` ]->(`07`),(`0`)-[:`HAS` ]->(`08`),(`0`)-[:`HAS` ]->(`09`)"
        for j in range(len(commands_elements)):
            if j == 0:
                for z in range(len(commands_elements[j])):
                    neo4j_relation_statement+= ",(`01`)-[:`Contact` ]->(`"+commands_elements[j][z]+"`)"
            if j == 1:
                for z in range(len(commands_elements[j])):
                    if len(commands_elements[j][z]) < 7:
                        Entity = commands_elements[j][z]
                        neo4j_relation_statement+= ",(`02`)-[:`EDUCATION_Entity` ]->("+Entity+")"
                    else:
                        neo4j_relation_statement+= ",("+Entity+")-[:`EDUCATION_ELEMENT` ]->(`"+commands_elements[j][z]+"`)"
            if j == 2:
                for z in range(len(commands_elements[j])):
                    if len(commands_elements[j][z]) < 7:
                        Entity = commands_elements[j][z]
                        neo4j_relation_statement+= ",(`03`)-[:`EXPERIENCE_Entity` ]->("+Entity+")"
                    else:
                        neo4j_relation_statement+= ",("+Entity+")-[:`EXPERIENCE_ELEMENT` ]->(`"+commands_elements[j][z]+"`)"
            if j == 3:
                for z in range(len(commands_elements[j])):
                    if len(commands_elements[j][z]) < 7:
                        Entity = commands_elements[j][z]
                        neo4j_relation_statement+= ",(`05`)-[:`CERTIFICATIONS_Entity` ]->("+Entity+")"
                    else:
                        neo4j_relation_statement+= ",("+Entity+")-[:`CERTIFICATIONS_ELEMENT` ]->(`"+commands_elements[j][z]+"`)"
            if j == 4:
                for z in range(len(commands_elements[j])):
                    if len(commands_elements[j][z]) < 7:
                        Entity = commands_elements[j][z]
                        neo4j_relation_statement+= ",(`06`)-[:`PROJECTS_Entity` ]->("+Entity+")"
                    else:
                        neo4j_relation_statement+= ",("+Entity+")-[:`PROJECTS_ELEMENT` ]->(`"+commands_elements[j][z]+"`)"
            if j == 5:
                for z in range(len(commands_elements[j])):
                    if len(commands_elements[j][z]) < 7:
                        Entity = commands_elements[j][z]
                        neo4j_relation_statement+= ",(`07`)-[:`AWARDS_Entity` ]->("+Entity+")"
                    else:
                        neo4j_relation_statement+= ",("+Entity+")-[:`AWARDS_ELEMENT` ]->(`"+commands_elements[j][z]+"`)"
            if j == 6:
                for z in range(len(commands_elements[j])):
                    if len(commands_elements[j][z]) < 7:
                        Entity = commands_elements[j][z]
                        neo4j_relation_statement+= ",(`08`)-[:`PUBLICATIONS_Entity` ]->("+Entity+")"
                    else:
                        neo4j_relation_statement+= ",("+Entity+")-[:`PUBLICATIONS_ELEMENT` ]->(`"+commands_elements[j][z]+"`)"
            if j == 7:
                for z in range(len(commands_elements[j])):
                    if len(commands_elements[j][z]) < 7:
                        Entity = commands_elements[j][z]
                        neo4j_relation_statement+= ",(`00`)-[:`REFERENCES_Entity` ]->("+Entity+")"
                    else:
                        neo4j_relation_statement+= ",("+Entity+")-[:`REFERENCES_ELEMENT` ]->(`"+commands_elements[j][z]+"`)"
        for i in range(len(skills_elements)):
            check_query = "MATCH (as:All_Skills)-[:CONTAIN_SKILL]->(z:"+skills_elements[i]+") RETURN as,z"
            if len(execute_match(check_query)) == 0:
                all_skills_relation_statement = "MATCH (as:All_Skills),(z:"+skills_elements[i]+") MERGE (as)-[:CONTAIN_SKILL]->(z)"
                all_skills_relation_statement_list.append(all_skills_relation_statement)
                cv_skill_relation_statement= "MATCH (r:Skills) where r.Resume_Id='"+ str(cv['CV_ID']) + "' MATCH (z:"+skills_elements[i]+") MERGE (r)-[:HAS_SKILL]->(z)"
                cv_skill_relation_statement_list.append(cv_skill_relation_statement)
            else:
                cv_skill_relation_statement= "MATCH (r:Skills) where r.Resume_Id='"+ str(cv['CV_ID']) + "' MATCH (z:"+skills_elements[i]+") MERGE (r)-[:HAS_SKILL]->(z)"
                cv_skill_relation_statement_list.append(cv_skill_relation_statement)

        full = neo4j_create_statement + neo4j_relation_statement

        return full,all_skills_relation_statement_list,cv_skill_relation_statement_list#,all_job_title_relation_statement_list,cv_job_title_relation_statement_list

    def execute_actions(**kwargs):
        from neo4j import GraphDatabase
        import json
        file_data = None
        file_name = '/home/spring/uploaded-cv/'+kwargs['dag_run'].conf['file_id']+'.json'
        with open(file_name, encoding='utf-8') as f:
            file_data = json.load(f)

        data_base_connection = GraphDatabase.driver(uri = "bolt://neo4j:7687", auth=("neo4j", "neo4j"))
        session = data_base_connection.session()
        ncs,ce,se,ie,ee,co,je = node_create_builder(file_data)
        full,asrst,csrst=node_relation_builder(file_data,ncs,ce,se,je)

        session.run(full)
        for i in asrst:
            session.run(i)
        for i in csrst:
            session.run(i)
        return 'Done'
    t3 = PythonOperator(
        task_id='t3',
        python_callable=execute_actions,
        provide_context=True,
        dag=dag,
    )
    prerequisite >> t1
    t1 >> t2
    t2 >> t3