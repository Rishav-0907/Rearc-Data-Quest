#!/usr/bin/env python3
import os
from aws_cdk import App
from cdk_python.cdk_python_stack import CdkPythonStack

app = App()
CdkPythonStack(app, "BLSDataPipelineStack",
    env={
        "account": os.getenv("CDK_DEFAULT_ACCOUNT"),
        "region": os.getenv("CDK_DEFAULT_REGION")
    }
)
app.synth() 