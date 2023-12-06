# Big-data-analytics-visualization-and-classification-with-pyspark-and-tableau
Big data analytics, visualization and classification of fire incidents happened at Toronto

Introduction

This project focuses on finding interesting insights from the fire-incidents dataset, obtained from Kaggle.com, by doing exploratory data analysis and visualization on the dataset. The dataset has the information on fire-incidents happened at Toronto, Canada during the period 2011 to 2018. The analysis mainly focuses on what all factors affected an increase in Civilian Casualties and Estimated Dollar Loss. Further, a classification task is done that would be useful in predicting Civilian Casualties and Estimated Dollar Loss.

Softwares and libraries used for this project are Tableau (for visualization) and PySpark (for analysis and Classification). Code is written in visual studio code editor, however it is executed in Linux terminal using pyspark.

Windows operating system is used for Tableau. A virtual machine with Linux operating system is used for execution of code in PySpark.

Description on the Dataset

Link to dataset:
https://www.kaggle.com/datasets/reihanenamdari/fire-incidents

This dataset contains 27 columns and 11214 rows.

Attributes and it’s Datatypes

Area_of_Origin(Datatype: String)
Business_Impact(Datatype: String)
Civilian_Casualties(Datatype: Int)
Count_of_Persons_Rescued(Datatype: Int)
Estimated_Dollar_Loss(Datatype: Int)
Estimated_Number_Of_Persons_Displaced(Datatype: Int)
Ext_agent_app_or_defer_time(Datatype: Timestamp)
Extent_Of_Fire(Datatype: String)
Fire_Alarm_System_Impact_on_Evacuation(Datatype: String)
Fire_Alarm_System_Operation(Datatype: String)
Fire_Alarm_System_Presence(Datatype: String)
Fire_Under_Control_Time(Datatype: Timestamp)
Ignition_Source(Datatype: String)
Incident_Station_Area(Datatype: Int)
Incident_Ward(Datatype: Int)
Last_TFS_Unit_Clear_Time(Datatype: Timestamp)
Latitude(Datatype: Double) 
Longitude(Datatype: Double)
Material_First_Ignited(Datatype: String)
Method_Of_Fire_Control(Datatype: String)
Possible_Cause(Datatype: String)
Property_Use(Datatype: String)
Smoke_Alarm_at_Fire_Origin_Alarm_Failure(Datatype: String)
Smoke_Alarm_at_Fire_Origin_Alarm_Type(Datatype: String)
Status_of_Fire_On_Arrival(Datatype: String)
TFS_Alarm_Time(Datatype: String)
TFS_Arrival_Time(Datatype: String)

Conclusion

Features responsible for an increase in number of Civilian Casualties are found to be Area of Origin,  Fire Alarm System Operation, Fire Alarm System Presence, Incident Station Area, Incident Ward, Possible Cause, Status of Fire On Arrival and Difference in Alarm Time and  Arrival Time. Features responsible for an increase in sum of Estimated Dollar Loss are found to be Business Impact, Extent Of Fire, Fire Alarm System Operation, Fire Alarm System Presence, Incident Station Area, Incident Ward, Material First Ignited, and Status of Fire On Arrival. 

The result of classification task done using Random Forest Classifier to predict Civilian Casualties shows that the model has 98% accuracy and test error of 0.012. The result of classification task done using Random Forest Classifier to predict to predict Estimated Dollar Loss shows that the model has 99% accuracy and test error of 0.007. This might be kind of overfitting. Need more work on this to find out a best model.

