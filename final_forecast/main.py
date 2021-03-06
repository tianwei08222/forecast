import os
import threading

# 主程序入口，顺序执行
# os.system("python forecast_companytype_jobnum.py")
# os.system("python forecast_companytype_salary.py")
# os.system("python forecast_education_jobnum.py")
# os.system("python forecast_education_salary.py")
# os.system("python forecast_jobnum.py")
# os.system("python forecast_salary.py")
# os.system("python forecast_workexper_jobnum_salary.py")
# os.system("python general_word_cloud.py")

# 多线程同时执行
print("机器学习模型启动，趋势预测开始")
thread1 = threading.Thread( target=os.system, args=("python forecast_companytype_jobnum.py",))
thread1.start()
thread1.join()
thread2 = threading.Thread( target=os.system, args=("python forecast_companytype_salary.py",))
thread2.start()
thread2.join()
thread3 = threading.Thread( target=os.system, args=("python forecast_education_jobnum.py",))
thread3.start()
thread3.join()
thread4 = threading.Thread( target=os.system, args=("python forecast_education_salary.py",))
thread4.start()
thread4.join()
thread5 = threading.Thread( target=os.system, args=("python forecast_jobnum.py",))
thread5.start()
thread5.join()
thread6 = threading.Thread( target=os.system, args=("python forecast_salary.py",))
thread6.start()
thread6.join()
thread7 = threading.Thread( target=os.system, args=("python forecast_workexper_jobnum_salary.py",))
thread7.start()
thread7.join()
thread8 = threading.Thread( target=os.system, args=("python general_word_cloud.py",))
thread8.start()
thread8.join()
