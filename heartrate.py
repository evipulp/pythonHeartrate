from datetime import datetime, time
import random
import pandas as pd
import simplejson as json



# {
# "user_id" : "abc",
# "timestamp" : 1587631419,
# "heart_rate" : 45,
# "respiration_rate" : 18,
# "activity" : 3,
# }


def simulate(timestamp):
    data = {"user_id" : "abc","timestamp" : 1587631419,"heart_rate" : 45,"respiration_rate" : 18,"activity" : 3}
    data['timestamp'] = timestamp
    data['heart_rate'] = random.randint(50,100)
    data['respiration_rate'] = random.randint(6,24)
    data['activity'] = random.randint(1,5)
    return data

def hourly_avg(data_15_min):
    # avg for hour based on 15 min df
    timestamp = data_15_min[0]['seg_start']
    ref_timestamp = timestamp
    temp_input = []
    hr_output = []
    print(timestamp)
    for data in data_15_min:
        timestamp = data['seg_start']
        if(timestamp < ref_timestamp + 3600):
            temp_input.append(data)
            hr_df = pd.DataFrame(temp_input)
            avg_rr=hr_df['avg_rr'].mean()
            avg_hr=hr_df['avg_hr'].mean()
            max_hr=hr_df['max_hr'].max()
            min_hr=hr_df['min_hr'].min()
            _data = {'user_id':data['user_id'],'seg_start': ref_timestamp,'seg_end':data['seg_end'],'avg_hr':avg_hr,'min_hr':min_hr,'max_hr':max_hr,'avg_rr':avg_rr}
            if(len(hr_output)):
                hr_output[len(hr_output)-1]= _data
            else:
                hr_output.append(data)
        else:
            print(timestamp)
            temp_inputData = []
            ref_timestamp = timestamp
            # df = None
            temp_inputData.append(data)
            hr_df = pd.DataFrame(temp_inputData)
            avg_rr=hr_df['avg_rr'].mean()
            avg_hr=hr_df['avg_hr'].mean()
            max_hr=hr_df['max_hr'].max()
            min_hr=hr_df['min_hr'].min()
            _data = {'user_id':data['user_id'],'seg_start': ref_timestamp,'seg_end':data['seg_end'],'avg_hr':avg_hr,'min_hr':min_hr,'max_hr':max_hr,'avg_rr':avg_rr}
            hr_output.append(_data)
    hr_df_output = pd.DataFrame(hr_output)
    print(hr_df_output)

    hourly_df = hr_df_output.to_csv("hourly_output_data.csv")



def processor():
    timestamp = 1624363140
    ref_timestamp = timestamp
    input_data = []
    temp_inputData = []
    output_data = []

    for _ in range(7200):
        data = simulate(timestamp)
        input_data.append(data)
        if(timestamp <= ref_timestamp + 899):
            temp_inputData.append(data)
            df = pd.DataFrame(temp_inputData)
            avg_rr=df['respiration_rate'].mean()
            avg_hr=df['heart_rate'].mean()
            max_hr=df['heart_rate'].max()
            min_hr=df['heart_rate'].min()
            _data = {'user_id':data['user_id'],'seg_start': ref_timestamp,'seg_end':timestamp,'avg_hr':avg_hr,'min_hr':min_hr,'max_hr':max_hr,'avg_rr':avg_rr}
            if(len(output_data)):
                output_data[len(output_data)-1]= _data
            else:
                output_data.append(data)
            
           
        else:
            print(timestamp)
            temp_inputData = []
            ref_timestamp = timestamp
            temp_inputData.append(data)
            df = pd.DataFrame(temp_inputData)
            avg_rr=df['respiration_rate'].mean()
            avg_hr=df['heart_rate'].mean()
            max_hr=df['heart_rate'].max()
            min_hr=df['heart_rate'].min()
            _data = {'user_id':data['user_id'],'seg_start': ref_timestamp,'seg_end':timestamp,'avg_hr':avg_hr,'min_hr':min_hr,'max_hr':max_hr,'avg_rr':avg_rr}
            output_data.append(_data)
        
        timestamp += 1

    output_data_df = pd.DataFrame(output_data)
    print(output_data_df)
    with open("input_data.json", "w") as f:
        json.dump(input_data,f)
    output_df = output_data_df.to_csv("15_min_output_data.csv")
    hourly_avg(output_data)

processor()
