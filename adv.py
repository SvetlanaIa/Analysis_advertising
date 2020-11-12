import json
import datetime
import pandas as pd


class Analysis():

    def __init__(self,res={}):
        self.res={}

    def extract(self,file_name):
        with open(file_name,encoding='utf-8') as f:
            data = json.load(f)
        return data  

    def transform(self,tranform_row):
        result=[]
        
        for row in tranform_row: #преобоазование каждой записи
            res={}
            res['id']=row['id']# аналогичным образом добавить столбцы, которые не требуют обработки
            res['created_at']=row['created_at']
            res['created_at_bq_timestamp']=datetime.datetime.fromtimestamp(row["created_at"]).strftime('%Y-%m-%d %H:%M:%S')
            res['created_at_year']=datetime.datetime.fromtimestamp(row["created_at"]).strftime('%Y')
            res['created_a_month']=datetime.datetime.fromtimestamp(row["created_at"]).strftime('%m')#проверить формат, написать расчет недели
            res['amo_city']=self.get_custom_fields_values(row)

            
            result.append(res)
        return result       


    def get_custom_fields_values(self,row):#вытаcкиваем все , поля , которые нужны из  'custom_fields_values'
        for custom_fields_values in row["custom_fields_values"]:
            if custom_fields_values['field_id']==512318:
                return custom_fields_values['values'][0]['value']
                # подумать много значений ?, возможно их тоже заменить на None        
   
        return ("None")    


    def create_dataframe(self,dict):#создания Dataframe (добавить столбцы)
        frame = pd.DataFrame(dict)
        return frame    
    

    # def load(self):
    #     pass      
   

if __name__ == '__main__':
    file_name="amo_json_2020_40.json"
    adv=Analysis()
    load_inform=adv.extract(file_name)
    transform_info=adv.transform(load_inform)
    frame=adv.create_dataframe(transform_info)
    print(frame)
        




