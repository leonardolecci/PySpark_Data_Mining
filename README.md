# PySpark_Data_Mining
With this repo we want to import data from the CSV file IBM_HR_Numbers and normalise the table throguh PySpark's function MinMaxScaler

METADATA
Here it is going to be described the variables that have been changed from string to integer

Attrition: "Yes" -> 0
           "No" -> 1

BusinessTravel: "Non_Travel" -> 1
                "Travel_Frequently" -> 2
                "Travel_Rarely" -> 3

Department: "HumanResources" -> 1
            "Sales" -> 2
            "Research & Development" -> 3

EductaionField: "HumanResources" -> 1
                "Other" -> 2
                "Technical Degree" -> 3
                "Marketing" -> 4
                "Medical" -> 5
                "Life Sciences" -> 6

Gender: "Female" -> 0
        "Male" -> 1

JobRole: "Human Resources" -> 1
         "Research Director" -> 2
         "Sales Representative" -> 3
         "Manager" -> 4
         "Healthcare Representative" -> 5
         "Manufacturing Director" -> 6
         "Laboratory Technician" -> 7
         "Research Scientist" -> 8 
         "Sales Executive" -> 9

MaritalStatus: "Divorced" -> 1
               "Single" -> 2
               "Married" -> 3

Over18: "No" -> 0
        "Yes" -> 1

OverTime: "True" -> 0
          "False" -> 1
