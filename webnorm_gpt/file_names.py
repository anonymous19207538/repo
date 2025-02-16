import os

project_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

data_folder = os.path.join(project_folder, "data")
train_data_file = os.path.join(data_folder, "Train_data.txt")
train_data_file_zst = os.path.join(data_folder, "Train_data.txt.zst")
test_data_file = os.path.join(data_folder, "Test_data.txt")
test_data_file_zst = os.path.join(data_folder, "Test_data.txt.zst")
train_data_modify_file = os.path.join(data_folder, "Train_data_modify.txt")
train_data_modify_file_zst = os.path.join(data_folder, "Train_data_modify.txt.zst")
ground_truth_txt = os.path.join(data_folder, "ground_truth.txt")
ground_truth_txt_zst = os.path.join(data_folder, "ground_truth.txt.zst")

generated_folder = os.path.join(project_folder, "generated")
commonsense_class_file = os.path.join(generated_folder, "commonsense_class.json")
commonsense_constraints = os.path.join(generated_folder, "commonsense_constraints.json")
commonsense_noclass = os.path.join(generated_folder, "commonsense_noclass.json")
data_constraints = os.path.join(generated_folder, "data_constraints.json")
trainticket_class_def = os.path.join(generated_folder, "trainticket_class_def.json")
trainticket_noclass_def = os.path.join(generated_folder, "trainticket_noclass_def.json")
trigger_constraints = os.path.join(generated_folder, "trigger_constraints.json")
openapi_file = os.path.join(generated_folder, "openAPI.json")
log_target_file = os.path.join(generated_folder, "log_target.json")
api_service_file = os.path.join(generated_folder, "API_service.json")
dataflow_file = os.path.join(generated_folder, "dataflow.json")
trigger_file = os.path.join(generated_folder, "trigger.json")
pre_defined_task_file = os.path.join(generated_folder, "pre_defined_task.json")
train_ticket_services_file = os.path.join(generated_folder, "train-ticket-services.txt")
train_ticket_services_file_zst = os.path.join(
    generated_folder, "train-ticket-services.txt.zst"
)

methods_folder = os.path.join(project_folder, "methods")
os.makedirs(methods_folder, exist_ok=True)

gpt_cache_sqlite = os.path.join(project_folder, "gpt_cache.sqlite")
gpt_log_file = os.path.join(project_folder, "gpt_log.log")
gpt_dump_folder = os.path.join(project_folder, "gpt_log_dump")
os.makedirs(gpt_dump_folder, exist_ok=True)

del os

if __name__ == "__main__":
    all_folders = list(
        filter(
            (lambda item: (not item[0].startswith("__") or not item[0].endswith("__"))),
            globals().items(),
        )
    )
    import os

    for name, value in all_folders:
        e = os.path.exists(value)
        e_msg = "" if e else "!!!!!!DOES NOT EXIST!!!!! "
        print(f"{e_msg}{name}: {value}")
