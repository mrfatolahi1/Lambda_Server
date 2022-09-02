import datetime
import json
import time
import ijson as ijson


input_path = "/home/mohammadreza/Downloads/20150801-20160801-activity/20150801-20151101-raw_user_activity.json"
output_path = "/home/mohammadreza/Downloads/out.json"

collected_datas = []
f = open(input_path)
for item in ijson.items(f, "item"):
    course_id, course_object = item
    # print(course_object)
    for user_id in course_object:
        user_object = course_object[user_id]
        # print(user_object)
        # time.sleep(2)
        for session_id in user_object:
            session_object = user_object[session_id]
            for user_activity in session_object:
                event, time1 = user_activity
                collected_data = json.dumps(
                    {
                        "course_id": course_id,
                        "user_id": user_id,
                        "session_id": session_id,
                        "activity_event": event,
                        "time": time1
                    }
                )
                # print(collected_data)

                collected_datas.append((collected_data, datetime.datetime.strptime(time1, '%Y-%m-%dT%H:%M:%S')))
    # time.sleep(2)

collected_datas.sort(key=lambda a: a[1])

f = open(output_path, "a")
for collected_data in collected_datas:
    f.write(collected_data[0])
    f.write("\n")
f.close()
