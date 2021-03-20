#!/usr/bin/env python

import swat
from sasctl import Session
from sasctl.tasks import register_model, publish_model


with swat.CAS('dsascontrol.org', 5570, 'robinswu', 'ucsb%fsj') as cas:
    # Load the regression actions in CAS
    cas.loadactionset('regression')

    # Upload the local CSV file to a CAS table
    tbl = cas.upload('/home/viya/data/Los_Angeles_houses_prices.csv').casTable

    # Model input features are everything except the target
    features = tbl.columns[tbl.columns != 'medv']

    # Fit a linear regression model in CAS and output an ASTORE
    tbl.glm(target='medv', inputs=list(features), savestate='model_table')

    astore = cas.CASTable('model_table')

    # Use sasctl to connect to SAS
    Session('hostname', 'username', 'password')

    # Register the model in SAS Model Manager, creating the "Boston Housing"
    # project if it doesn't already exist
    model = register_model(astore, 'Linear Regression', 'Los Angeles Housing', force=True)

    # Publish the model to a real-time scoring engine
    module = publish_model(model, 'maslocal')

    # Pass a row of data to MAS and receive the predicted result.
    first_row = tbl.head(1)
    result = module.score(first_row)
    print(result)
