from db_model.MASTER_MODEL import select_data, insert_data,update_data,delete_data


async def regions_list():
   try:
       data = select_data("md_lo_regions", "*")
       print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>.",data)
       return data
   except Exception as e:
         raise e
     

async def sub_regions_list(regions=None):
   try:
       condition = f"region_id={regions}" if regions is not None else None
       data = select_data("md_lo_subregions", "*",condition)
       return data
   except Exception as e:
         raise e
     

async def countries_list(subregion_id=None,region_id=None):
    try:
         condition = f"region_id={region_id}" if region_id is not None else None
         condition2 =    condition+f" AND subregion_id={subregion_id} " if subregion_id is not None else None  if condition is not None else f"subregion_id={subregion_id} " if subregion_id is not None else None
         data = select_data("md_lo_countries", "*",condition2)
         return data
    except Exception as e:
            raise e
        

async def states_list(country_id=None):
    try:
        condition = f"country_id={country_id}" if country_id is not None else None
        data = select_data("md_lo_states", "*",condition)
        return data
    except Exception as e:
        raise e
async def cities_list(state_id=None):
    try:
        condition = f"state_id={state_id}" if state_id is not None else None
        data = select_data("md_lo_cities", "*",condition)
        return data
    except Exception as e:
        raise e