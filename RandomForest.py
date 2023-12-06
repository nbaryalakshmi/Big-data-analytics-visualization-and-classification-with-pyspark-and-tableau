from pyspark.sql.functions import udf,col,trim,lit
from pyspark.sql.types import StringType
from pyspark.sql.functions import unix_timestamp
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import FeatureHasher
from pyspark.ml.feature import ChiSqSelector
from pyspark.ml.feature import VectorAssembler
import numpy as np

def get_Casualties_bins(val):
    if val>=0 and val<=5:
        return '0-5'
    elif val>5 and val<=10:
        return '6-10'
    elif val>10 and val<=15:
        return '11-15'    

def get_Dollarloss_bins(val):    
    if val<=500000:#5 lakhs
        return '0-500000'
    elif val<=1000000:#10 lakhs
        return '500001-1000000'
    elif val<=2000000:#20 lakhs
        return '1000001-2000000'
    else:
        return 'greater than 2000000'
#Select Final features
def get_FinalFeatures(df_data,col_bin):
    sel_cols=['Area_of_Origin','Business_Impact',		
	'Extent_Of_Fire',	'Fire_Alarm_System_Impact_on_Evacuation',
    'Fire_Alarm_System_Operation',	'Fire_Alarm_System_Presence',	'Ignition_Source',
    'Incident_Station_Area',	'Incident_Ward',	'Material_First_Ignited',	'Method_Of_Fire_Control',	'Possible_Cause',	'Property_Use',
	'Smoke_Alarm_at_Fire_Origin_Alarm_Failure',	'Smoke_Alarm_at_Fire_Origin_Alarm_Type'	,
    'Status_of_Fire_On_Arrival','Diff_AlarmTime_ArrivalTime',col_bin]#
    df_data_clsfr=df_data.select(sel_cols)
    feature_cols=[]
    for clm in df_data_clsfr.columns:
        strOutCol=clm+'_index'
        strnIndxr = StringIndexer(inputCol =clm, outputCol =strOutCol )
        strnIndxr_model = strnIndxr.fit(df_data_clsfr)
        df_data_clsfr = strnIndxr_model.transform(df_data_clsfr)
        feature_cols.append(strOutCol)
    for clm in sel_cols:
        df_data_clsfr = df_data_clsfr.drop(clm)
    feature_cols.remove(col_bin+'_index')
    vec_assmblr = VectorAssembler(inputCols = feature_cols, outputCol="features")
    vec_assmblr_df = vec_assmblr.transform(df_data_clsfr)
    selector = ChiSqSelector(numTopFeatures=8,featuresCol='features', fpr=0.05, outputCol="selectedFeatures",labelCol= col_bin+"_index")
    model = selector.fit(vec_assmblr_df)
    vec_assmblr_df=model.transform(vec_assmblr_df)
    print('Selected Features are ',np.array(df_data_clsfr.columns)[model.selectedFeatures])
    final_features=[]
    for col in np.array(df_data_clsfr.columns)[model.selectedFeatures]:
        final_features.append(col.replace('_index',''))
    return final_features
#Random Forest Classifier
def try_RandomForest(final_features,col_bin):    
    df_data_clsfr=df_data.select(final_features+[col_bin])
    labelIndexer = StringIndexer(inputCol=col_bin, outputCol="indexedLabel").fit(df_data_clsfr)
    hasher = FeatureHasher(inputCols=final_features, outputCol="selectedFeatures")
    df_data_clsfr = hasher.transform(df_data_clsfr)
    featureIndexer =  VectorIndexer(inputCol="selectedFeatures", outputCol="indexedFeatures", maxCategories=4).fit(df_data_clsfr)
    #Split the data into training and test sets (40% held out for testing)
    (trainingData, testData) = df_data_clsfr.randomSplit([0.6, 0.4])
    #Train a RandomForest model.
    rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", numTrees=10)
    #Convert indexed labels back to original labels.
    labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",labels=labelIndexer.labels)
    #Chain indexers and forest in a Pipeline
    pipeline = Pipeline(stages=[labelIndexer, featureIndexer, rf, labelConverter])
    #Train model.  This also runs the indexers.
    model = pipeline.fit(trainingData)
    #Make predictions.
    predictions = model.transform(testData)
    #Select example rows to display.
    predictions.select("predictedLabel", col_bin, "selectedFeatures").show(5)
    #Select (prediction, true label) and compute test error
    evaluator = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print("Accuracy",accuracy,"Test Error = %g" % (1.0 - accuracy))
    rfModel = model.stages[2]
    print(rfModel)  


    
if __name__ == "__main__":
    df_data=spark.read.csv('Data/Fire-Incidents.csv', inferSchema =True, header=True)
    df_data=df_data.filter(col('Civilian_Casualties')>0)
    print('No. of rows: ',df_data.count(), 'No. of columns: ',len(df_data.columns))
    #############
    df_data=df_data.na.fill({'Incident_Ward':-1})
    df_data=df_data.withColumn('Diff_AlarmTime_ArrivalTime',unix_timestamp(col('TFS_Arrival_Time'))-unix_timestamp(col('TFS_Alarm_Time')))
    df_data.select('Diff_AlarmTime_ArrivalTime').show(5)
    #==========================Civilian_Casualties
    int_min_caslty=df_data.agg({"Civilian_Casualties": 'min'}).collect()[0][0]
    int_max_caslty=df_data.agg({"Civilian_Casualties": 'max'}).collect()[0][0]
    print('Min Value: ',int_min_caslty,'Max Value: ',int_max_caslty)
    #Convert python udf to pyspark udf
    udf_casualty_bins = udf(lambda x:get_Casualties_bins(x),StringType())
    df_data=df_data.withColumn('Civilian_Casualties_bins',udf_casualty_bins(col('Civilian_Casualties')))
    df_data.select('Civilian_Casualties','Civilian_Casualties_bins').show(5)
    #check for null values
    df_data.filter(df_data['Civilian_Casualties_bins'].isNull()).count()
    final_features=get_FinalFeatures(df_data,'Civilian_Casualties_bins')
    try_RandomForest(final_features,'Civilian_Casualties_bins')#Random Forest
    #################################
    #Estimated_Dollar_Loss
    int_min_DolrLos=df_data.agg({"Estimated_Dollar_Loss": 'min'}).collect()[0][0]
    int_max_DolrLos=df_data.agg({"Estimated_Dollar_Loss": 'max'}).collect()[0][0]
    print('Min Value: ',int_min_DolrLos,'Max Value: ',int_max_DolrLos)
    #Convert python udf to pyspark udf
    udf_DolrLos_bins = udf(lambda x:get_Dollarloss_bins(x),StringType())
    df_data=df_data.withColumn('Estimated_Dollar_Loss_bins',udf_DolrLos_bins(col('Estimated_Dollar_Loss')))
    df_data.select('Estimated_Dollar_Loss','Estimated_Dollar_Loss_bins').show(5)
    #check for null values
    df_data.filter(df_data['Estimated_Dollar_Loss_bins'].isNull()).count()
    final_features_dollar=get_FinalFeatures(df_data,'Estimated_Dollar_Loss_bins')
    try_RandomForest(final_features_dollar,'Estimated_Dollar_Loss_bins')#Random Forest







