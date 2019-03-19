# HA17493 - Daily Patient Discharge Report

### What is This?

This is a Python process that queries for all patients discharged from the hospital yesterday. It then creates an HTML email by discharge location and notifies the relevant PDLs and people for each location. It also calculates the average amount of time between the discharge order and the actual discharge and average discharge time (of day) per location.