import json
input_params={}
raw_folder_path="/mnt/mavendataguy/input/ecom_growth"
raw_folder_path_products="/mnt/mavendataguy/input/ecom_growth/products"
raw_folder_path_sessions="/mnt/mavendataguy/input/ecom_growth/website_sessions"
processed_folder_path="/mnt/mavendataguy/output/ecom_growth"
presentation_folder_path = "/mnt/mavendataguy/output/present"
v_file_date="2021-12-15"
input_params['v_file_date']=v_file_date
input_params['raw_folder_path']=raw_folder_path

with open('/mnt/mavendataguy/input/params.json','w') as fp:
  json.dump(input_params,fp)

with open('/mnt/mavendataguy/input/params.json','r') as fp:
  params_data=json.load(fp)
  
print(params_data['raw_folder_path'])