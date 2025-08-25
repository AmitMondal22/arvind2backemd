from db_model.MASTER_MODEL import select_data

async def list_unit():
    try:
        data=select_data("md_unit","unit_id, unit,unit_name",None,"unit ASC")
        return data
    except Exception as e:
        raise e