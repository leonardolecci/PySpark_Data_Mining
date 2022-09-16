# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE_MAGIC_CELL
# Automatically replaced inline charts by "no-op" charts
# %pylab inline
import matplotlib
matplotlib.use("Agg")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
import dataiku
import dataiku.spark as dkuspark
import pyspark
from pyspark.sql import SQLContext

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Load PySpark
sc = pyspark.SparkContext()
sqlContext = SQLContext(sc)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Read the descriptor of the Dataiku dataset
dataset = dataiku.Dataset("IBM_HR_Numbers_S3")
# And read it as a Spark dataframe
df = dkuspark.get_dataframe(sqlContext, dataset)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
#getting columns name
col = df.columns
print(col)
# UDF for converting column type from vector to double type
unlist = udf(lambda x: round(float(list(x)[0]),3), DoubleType())
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Iterating over columns to be scaled
for i in ['Age', 'Attrition', 'BusinessTravel', 'DailyRate', 'Department', 'DistanceFromHome', 'Education', 'EducationField', 'EmployeeCount', 'EmployeeNumber', 'EnvironmentSatisfaction', 'Gender', 'HourlyRate', 'JobInvolvement', 'JobLevel', 'JobRole', 'JobSatisfaction', 'MaritalStatus', 'MonthlyIncome', 'MonthlyRate', 'NumCompaniesWorked', 'Over18', 'OverTime', 'PercentSalaryHike', 'PerformanceRating', 'RelationshipSatisfaction', 'StandardHours', 'StockOptionLevel', 'TotalWorkingYears', 'TrainingTimesLastYear', 'WorkLifeBalance', 'YearsAtCompany', 'YearsInCurrentRole', 'YearsSinceLastPromotion', 'YearsWithCurrManager']:
    # VectorAssembler Transformation - Converting column to vector type
    assembler = VectorAssembler(inputCols=[i],outputCol=i+"_Vect")

    # MinMaxScaler Transformation
    scaler = MinMaxScaler(inputCol=i+"_Vect", outputCol=i+"_Scaled")

    # Pipeline of VectorAssembler and MinMaxScaler
    pipeline = Pipeline(stages=[assembler, scaler])

    # Fitting pipeline on dataframe
    df = pipeline.fit(df).transform(df).withColumn(i+"_Scaled", unlist(i+"_Scaled")).drop(i+"_Vect")

# CODE TO CHANGE IN THE RECIPE IN DATAIKU BEFORE RUNNING IT
## Recipe outputs

#ibm_hr_numbers_standardized_sql = dataiku.Dataset("IBM_HR_Numbers_Standardized_SQL")
#dkuspark.write_with_schema(ibm_hr_numbers_standardized_sql, df) here instead of "Spark_DataFrame", substitute it with the actual name of the dataframe