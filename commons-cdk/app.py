#!/usr/bin/env python3
import aws_cdk as cdk
from utilities.main import MyAppStack


app = cdk.App()
MyAppStack(app, "commons-cdk")

app.synth()
