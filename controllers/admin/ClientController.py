
from db_model.MASTER_MODEL import select_data, insert_data,update_data,delete_data
from utils.date_time_format import get_current_datetime


def add_organization(organization):
    try:
              
        current_datetime = get_current_datetime()
        columns = "client_id,organization_name, created_by, created_at"
        value = f"{organization.client_id},'{organization.organization_name}', {organization.created_by},'{current_datetime}'"
        organization_id = insert_data("md_organization", columns, value)
        if organization_id is None:
            raise ValueError("organization registration failed")
        else:
            user_data = {"user_id": organization_id, "organization_name": organization.organization_name, "created_by": organization.created_by}
        return user_data
    except Exception as e:
        raise e
    
    
def add_projects(project, user_data):
    try:
              
        current_datetime = get_current_datetime()
        columns = "organization_id, project_name, created_by, created_at"
        value = f"{project.organization_id},'{project.project_name}', {user_data['user_id']},'{current_datetime}'"
        project_id = insert_data("md_project", columns, value)
        if project_id is None:
            raise ValueError("project creation failed")
        else:
            user_data = {"project_id": project_id, "project_name": project.project_name, "created_by": user_data['user_id']}
        return user_data
    except Exception as e:
        raise e
    

def list_organization(params,user_data):
    try:
        if user_data["user_type"]=='U' or user_data["user_type"]=='O':
            condition =f"client_id = {user_data['client_id']} AND organization_id = {user_data['organization_id']}"
        elif user_data["user_type"]=='C':
            condition =f"client_id = {user_data['client_id']}"
            
        select="client_id,organization_id, organization_name, created_by, DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') AS created_at"
        data = select_data("md_organization", select,condition)
        return data
    except Exception as e:
        raise e
def manage_organization_project(user_data):
    # try:
        if user_data["user_type"]=='U' or user_data["user_type"]=='O':
            condition =f"client_id = {user_data['client_id']} AND organization_id = {user_data['organization_id']}"
            condition2 =f"organization_id = {user_data['organization_id']}"
        elif user_data["user_type"]=='C':
            condition =f"client_id = {user_data['client_id']}"
            condition2 = None
            
        select="client_id,organization_id, organization_name, created_by, DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') AS created_at"
        select2="project_id, organization_id, project_name, created_by"
        organizations = select_data("md_organization", select,condition)
        projects = select_data("md_project", select2,condition2)
        
        
        
        project_dict = {}
        for project in projects:
            org_id = project["organization_id"]
            if org_id not in project_dict:
                project_dict[org_id] = []
            project_dict[org_id].append(project)
        
        # Nest projects inside organizations
        for org in organizations:
            org["projects"] = project_dict.get(org["organization_id"], [])
            
            
        return organizations
    # except Exception as e:
    #     raise e
    
    
def list_project(user_data):
    try:
        if user_data["user_type"]=='U' or user_data["user_type"]=='O':
             condition =f"a.organization_id = b.organization_id AND a.client_id = {user_data['client_id']} AND a.organization_id = {user_data['organization_id']} "
            # condition =f"a.client_id = {user_data['client_id']} AND a.organization_id = {user_data['organization_id']} AND "

        elif user_data["user_type"]=='C':
            condition =f"a.client_id = {user_data['client_id']}"
        select="b.project_name, b.project_id, b.organization_id, a.organization_name, DATE_FORMAT(b.created_at, '%Y-%m-%d %H:%i:%s') AS created_at"
        data = select_data("md_organization a, md_project b", select,condition)
        return data
    except Exception as e:
        raise e
    
    

def edit_organization(organization):
    try:
        condition = f"organization_id = {organization.organization_id} AND client_id={organization.client_id}"
        columns={"organization_name":organization.organization_name,"created_by":organization.created_by,"updated_at":get_current_datetime()}
        data = update_data("md_organization", columns, condition)
        return data
    except Exception as e:
        raise e
    

def edit_projects(project,user_data):
    try:
        condition = f"project_id = {project.project_id}"
        columns={"project_name":project.project_name,"organization_id":project.organization_id,"created_by":user_data['user_id'],"updated_at":get_current_datetime()}
        data = update_data("md_project", columns, condition)
        return data
    except Exception as e:
        raise e


def delete_organization(organization):
    try:
        condition = f"organization_id = {organization.organization_id} AND client_id={organization.client_id}"
        data = delete_data("md_organization", condition)
        return data
    except Exception as e:
        raise e
    
    
    
def delete_manage_projects(organization):
    try:
        condition = f"project_id = {organization.project_id}"
        data = delete_data("md_project", condition)
        return data
    except Exception as e:
        raise e
    

# 
# def energy_data(energy_data):
#     try:
#         table="td_weather_data AS ed, md_device AS md"
#         select="ed.energy_data_id, ed.device_id, ed.device, ed.do_channel, ed.e1, ed.e2, ed.e3, ed.r, ed.y, ed.b, ed.r_y, ed.y_b, ed.b_r, ed.curr1, ed.curr2, ed.curr3, ed.activep1, ed.activep2, ed.activep3, ed.apparentp1, ed.apparentp2, ed.apparentp3, ed.pf1, ed.pf2, ed.pf3, ed.freq, ed.reactvp1, ed.reactvp2, ed.reactvp3, ed.avaragevln, ed.avaragevll, ed.avaragecurrent, ed.totkw, ed.totkva, ed.totkvar, ed.runhr, ed.date, ed.time , DATE_FORMAT(ed.created_at, '%Y-%m-%d %H:%i:%s') AS created_at,md.device,md.device_type, md.meter_type, md.device_name"
        
#         condition = f"ed.device_id=md.device_id AND ed.device_id = {energy_data.device_id} AND ed.device = '{energy_data.device}' AND ed.date BETWEEN '{energy_data.start_date}' AND '{energy_data.end_date}'"
#         order_by="ed.date DESC, ed.time DESC"
#         data=select_data(table, select,condition,order_by)
#         return data
#     except Exception as e:
#         raise e