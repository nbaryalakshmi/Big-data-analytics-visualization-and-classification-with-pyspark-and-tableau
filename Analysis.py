from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import udf,col,trim

if __name__ == "__main__":
    #Read dataset
    df_data=spark.read.csv('Data/Fire-Incidents.csv', inferSchema =True, header=True)
    #Check number of rows and columns
    print('No. of rows: ',df_data.count(), 'No. of columns: ',len(df_data.columns))
    #Check column names
    df_data.columns
    #Check datatypes of each column
    df_data.dtypes
    #Display first 10 rows and 5 selected columns
    df_data.select('Area_of_Origin','Civilian_Casualties','Count_of_Persons_Rescued','Estimated_Dollar_Loss','TFS_Arrival_Time').show(10)
    #Check null values
    for col in df_data.columns:
        count_null=df_data.filter(df_data[col].isNull()).count()
        if count_null>0:
            print(col,' has ',count_null,' null values.')
    #Fill null values
    df_data.select('Incident_Ward').distinct().collect()
    df_data=df_data.na.fill({'Incident_Ward':-1})
    #test null value count
    df_data.filter(df_data['Incident_Ward'].isNull()).count()

    #clean string columns, whitespaces #pending
    #Find out string columns
    lst_strCols=[]
    for col,dtype in df_data.dtypes:
        if dtype=='string':
            lst_strCols.append(col)
    print(lst_strCols)
    for clm in lst_strCols:
        df_data = df_data.withColumn(clm, trim(col(clm)))

    ###Area_of_Origin
    #Let's check the top area of origin of fire 
    df_areaOfOrigin_cnt=df_data.groupby('Area_of_Origin').count().orderBy('count',ascending=False)
    df_areaOfOrigin_cnt.show(5)
    #Area_of_Origin => #Civilian_Casualties,Estimated_Dollar_Loss
    df_origin_casulty_dolrloss=df_data.select('Area_of_Origin','Civilian_Casualties','Estimated_Dollar_Loss').groupby('Area_of_Origin').sum()
    df_origin_casulty_dolrloss.orderBy(['sum(Civilian_Casualties)'],ascending=False).show(5)
    df_origin_casulty_dolrloss.orderBy(['sum(Estimated_Dollar_Loss)'],ascending=False).show(5)
    ###Business_Impact
    df_bsns_impct_cnt=df_data.groupby('Business_Impact').count().orderBy('count',ascending=False)
    df_bsns_impct_cnt.show(5)
    #Business_Impact => #Civilian_Casualties,Estimated_Dollar_Loss
    df_bsns_impct_casulty_dolrloss=df_data.select('Business_Impact','Civilian_Casualties','Estimated_Dollar_Loss').groupby('Business_Impact').sum()
    df_bsns_impct_casulty_dolrloss.orderBy(['sum(Civilian_Casualties)'],ascending=False).show(5)
    df_bsns_impct_casulty_dolrloss.orderBy(['sum(Estimated_Dollar_Loss)'],ascending=False).show(5)
    ###Extent_Of_Fire
    df_extntOfFire_cnt=df_data.groupby('Extent_Of_Fire').count().orderBy('count',ascending=False)
    df_extntOfFire_cnt.show(5)
    #Extent_Of_Fire => #Civilian_Casualties,Estimated_Dollar_Loss
    df_extntOfFire_casulty_dolrloss=df_data.select('Extent_Of_Fire','Civilian_Casualties','Estimated_Dollar_Loss').groupby('Extent_Of_Fire').sum()
    df_extntOfFire_casulty_dolrloss.orderBy(['sum(Civilian_Casualties)'],ascending=False).show(5)
    df_extntOfFire_casulty_dolrloss.orderBy(['sum(Estimated_Dollar_Loss)'],ascending=False).show(5)
    ###Fire_Alarm_System_Impact_on_Evacuation
    df_fireAlrmImpct_cnt=df_data.groupby('Fire_Alarm_System_Impact_on_Evacuation').count().orderBy('count',ascending=False)
    df_fireAlrmImpct_cnt.show(5)
    #Fire_Alarm_System_Impact_on_Evacuation => #Civilian_Casualties,Estimated_Dollar_Loss
    df_fireAlrmImpct_casulty_dolrloss=df_data.select('Fire_Alarm_System_Impact_on_Evacuation','Civilian_Casualties','Estimated_Dollar_Loss').groupby('Fire_Alarm_System_Impact_on_Evacuation').sum()
    df_fireAlrmImpct_casulty_dolrloss.orderBy(['sum(Civilian_Casualties)'],ascending=False).show(5)
    df_fireAlrmImpct_casulty_dolrloss.orderBy(['sum(Estimated_Dollar_Loss)'],ascending=False).show(5)
    ###Fire_Alarm_System_Operation
    df_fireAlrmOprtn_cnt=df_data.groupby('Fire_Alarm_System_Operation').count().orderBy('count',ascending=False)
    df_fireAlrmOprtn_cnt.show(5)
    #Fire_Alarm_System_Operation => #Civilian_Casualties,Estimated_Dollar_Loss
    df_fireAlrmOprtn_casulty_dolrloss=df_data.select('Fire_Alarm_System_Operation','Civilian_Casualties','Estimated_Dollar_Loss').groupby('Fire_Alarm_System_Operation').sum()
    df_fireAlrmOprtn_casulty_dolrloss.orderBy(['sum(Civilian_Casualties)'],ascending=False).show(5)
    df_fireAlrmOprtn_casulty_dolrloss.orderBy(['sum(Estimated_Dollar_Loss)'],ascending=False).show(5)
    ###Fire_Alarm_System_Presence
    df_fireAlrmPrsnc_cnt=df_data.groupby('Fire_Alarm_System_Presence').count().orderBy('count',ascending=False)
    df_fireAlrmPrsnc_cnt.show(5)
    #Fire_Alarm_System_Presence => #Civilian_Casualties,Estimated_Dollar_Loss
    df_fireAlrmPrsnc_casulty_dolrloss=df_data.select('Fire_Alarm_System_Presence','Civilian_Casualties','Estimated_Dollar_Loss').groupby('Fire_Alarm_System_Presence').sum()
    df_fireAlrmPrsnc_casulty_dolrloss.orderBy(['sum(Civilian_Casualties)'],ascending=False).show(5)
    df_fireAlrmPrsnc_casulty_dolrloss.orderBy(['sum(Estimated_Dollar_Loss)'],ascending=False).show(5)
    ###Fire_Alarm_System_Presence => Fire_Alarm_System_Operation
    df_alrm_prsnc_oprtn=df_data.groupby('Fire_Alarm_System_Presence').pivot('Fire_Alarm_System_Operation').count()
    df_alrm_prsnc_oprtn.show()

    #Ignition_Source
    df_igntnSrc_cnt=df_data.groupby('Ignition_Source').count().orderBy('count',ascending=False)
    df_igntnSrc_cnt.show(5)
    #Ignition_Source => #Civilian_Casualties,Estimated_Dollar_Loss
    df_igntnSrc_casulty_dolrloss=df_data.select('Ignition_Source','Civilian_Casualties','Estimated_Dollar_Loss').groupby('Ignition_Source').sum()
    df_igntnSrc_casulty_dolrloss.orderBy(['sum(Civilian_Casualties)'],ascending=False).show(5)
    df_igntnSrc_casulty_dolrloss.orderBy(['sum(Estimated_Dollar_Loss)'],ascending=False).show(5)
    #Incident_Station_Area & Incident_Ward
    #Incident_Station_Area & Incident_Ward => #Civilian_Casualties,Estimated_Dollar_Loss
    df_incdntStnWrd_casulty_dolrloss=df_data.select('Incident_Station_Area' ,'Civilian_Casualties','Estimated_Dollar_Loss').groupby(['Incident_Station_Area']).sum()
    df_incdntStnWrd_casulty_dolrloss.orderBy(['sum(Civilian_Casualties)'],ascending=False).show(5)
    df_incdntStnWrd_casulty_dolrloss.orderBy(['sum(Estimated_Dollar_Loss)'],ascending=False).show(5)
    df_incdntStnWrd_casulty_dolrloss=df_data.select('Incident_Ward','Civilian_Casualties','Estimated_Dollar_Loss').groupby(['Incident_Ward']).sum()
    df_incdntStnWrd_casulty_dolrloss.orderBy(['sum(Civilian_Casualties)'],ascending=False).show(5)
    df_incdntStnWrd_casulty_dolrloss.orderBy(['sum(Estimated_Dollar_Loss)'],ascending=False).show(5)
    #Latitude & Longitude
    #Material_First_Ignited
    df_mtrlFrstIgntd_cnt=df_data.groupby('Material_First_Ignited').count().orderBy('count',ascending=False)
    df_mtrlFrstIgntd_cnt.show(5)
    #Material_First_Ignited => #Civilian_Casualties,Estimated_Dollar_Loss
    df_mtrlFrstIgntd_casulty_dolrloss=df_data.select('Material_First_Ignited','Civilian_Casualties','Estimated_Dollar_Loss').groupby('Material_First_Ignited').sum()
    df_mtrlFrstIgntd_casulty_dolrloss.orderBy(['sum(Civilian_Casualties)'],ascending=False).show(5)
    df_mtrlFrstIgntd_casulty_dolrloss.orderBy(['sum(Estimated_Dollar_Loss)'],ascending=False).show(5)
    #Method_Of_Fire_Control
    df_mthdFrCntrl_cnt=df_data.groupby('Method_Of_Fire_Control').count().orderBy('count',ascending=False)
    df_mthdFrCntrl_cnt.show(5)
    #Method_Of_Fire_Control => #Civilian_Casualties,Estimated_Dollar_Loss
    df_mthdFrCntrl_casulty_dolrloss=df_data.select('Method_Of_Fire_Control','Civilian_Casualties','Estimated_Dollar_Loss').groupby('Method_Of_Fire_Control').sum()
    df_mthdFrCntrl_casulty_dolrloss.orderBy(['sum(Civilian_Casualties)'],ascending=False).show(5)
    df_mthdFrCntrl_casulty_dolrloss.orderBy(['sum(Estimated_Dollar_Loss)'],ascending=False).show(5)
    #Possible_Cause
    df_psblCaus_cnt=df_data.groupby('Possible_Cause').count().orderBy('count',ascending=False)
    df_psblCaus_cnt.show(5)
    #Possible_Cause => #Civilian_Casualties,Estimated_Dollar_Loss
    df_psblCaus_casulty_dolrloss=df_data.select('Possible_Cause','Civilian_Casualties','Estimated_Dollar_Loss').groupby('Possible_Cause').sum()
    df_psblCaus_casulty_dolrloss.orderBy(['sum(Civilian_Casualties)'],ascending=False).show(5)
    df_psblCaus_casulty_dolrloss.orderBy(['sum(Estimated_Dollar_Loss)'],ascending=False).show(5)
    #Property_Use => Possible_Cause #not coded
    df_prprtyUseCaus_cnt=df_data.select('Property_Use','Civilian_Casualties','Estimated_Dollar_Loss').groupby(['Property_Use']).sum()
    df_prprtyUseCaus_cnt.orderBy(['sum(Civilian_Casualties)'],ascending=False).show(5)
    df_prprtyUseCaus_cnt.orderBy(['sum(Estimated_Dollar_Loss)'],ascending=False).show(5)
    #Smoke_Alarm_at_Fire_Origin_Alarm_Failure & Smoke_Alarm_at_Fire_Origin_Alarm_Type
    #df_smkAlmFrTypFlr_cnt=df_data.select('Smoke_Alarm_at_Fire_Origin_Alarm_Type','Smoke_Alarm_at_Fire_Origin_Alarm_Failure').groupby(['Smoke_Alarm_at_Fire_Origin_Alarm_Type','Smoke_Alarm_at_Fire_Origin_Alarm_Failure']).count().orderBy('count',ascending=False)
    #df_smkAlmFrTypFlr_cnt.show(5)
    #Smoke_Alarm_at_Fire_Origin_Alarm_Failure
    #Smoke_Alarm_at_Fire_Origin_Alarm_Failure => #Civilian_Casualties,Estimated_Dollar_Loss
    df_smkAlmFrFlr_casulty_dolrloss=df_data.select('Smoke_Alarm_at_Fire_Origin_Alarm_Failure','Civilian_Casualties','Estimated_Dollar_Loss').groupby('Smoke_Alarm_at_Fire_Origin_Alarm_Failure').sum()
    df_smkAlmFrFlr_casulty_dolrloss.orderBy(['sum(Civilian_Casualties)'],ascending=False).show(5)
    df_smkAlmFrFlr_casulty_dolrloss.orderBy(['sum(Estimated_Dollar_Loss)'],ascending=False).show(5)
    #Smoke_Alarm_at_Fire_Origin_Alarm_Type
    #Smoke_Alarm_at_Fire_Origin_Alarm_Type => #Civilian_Casualties,Estimated_Dollar_Loss
    df_smkAlmFrTyp_casulty_dolrloss=df_data.select('Smoke_Alarm_at_Fire_Origin_Alarm_Type','Civilian_Casualties','Estimated_Dollar_Loss').groupby('Smoke_Alarm_at_Fire_Origin_Alarm_Type').sum()
    df_smkAlmFrTyp_casulty_dolrloss.orderBy(['sum(Civilian_Casualties)'],ascending=False).show(5)
    df_smkAlmFrTyp_casulty_dolrloss.orderBy(['sum(Estimated_Dollar_Loss)'],ascending=False).show(5)
    #Status_of_Fire_On_Arrival
    #Status_of_Fire_On_Arrival => #Civilian_Casualties,Estimated_Dollar_Loss
    df_stsFrOnArvl_casulty_dolrloss=df_data.select('Status_of_Fire_On_Arrival','Civilian_Casualties','Estimated_Dollar_Loss').groupby('Status_of_Fire_On_Arrival').sum()
    df_stsFrOnArvl_casulty_dolrloss.orderBy(['sum(Civilian_Casualties)'],ascending=False).show(5)
    df_stsFrOnArvl_casulty_dolrloss.orderBy(['sum(Estimated_Dollar_Loss)'],ascending=False).show(5)
    #TFS_Alarm_Time & TFS_Arrival_Time
    #TFS_Alarm_Time & TFS_Arrival_Time => diff => A
    df_data=df_data.withColumn('Diff_AlarmTime_ArrivalTime',unix_timestamp(col('TFS_Arrival_Time'))-unix_timestamp(col('TFS_Alarm_Time')))
    df_data.select('Diff_AlarmTime_ArrivalTime').show(5)#in seconds
    #Diff_AlarmTime_ArrivalTime => #Civilian_Casualties,Estimated_Dollar_Loss
    df_diffAlrmArvlTim_casulty_dolrloss=df_data.select('Diff_AlarmTime_ArrivalTime','Civilian_Casualties','Estimated_Dollar_Loss').groupby('Diff_AlarmTime_ArrivalTime').sum('Civilian_Casualties','Estimated_Dollar_Loss')
    df_diffAlrmArvlTim_casulty_dolrloss.orderBy(['sum(Civilian_Casualties)'],ascending=False).show(5)
    df_diffAlrmArvlTim_casulty_dolrloss.orderBy(['sum(Estimated_Dollar_Loss)'],ascending=False).show(5)

