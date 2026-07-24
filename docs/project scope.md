# Project Scope

## Project name
Enterprise Banking Data Platform

## Objective
Create a production-style analytics platform for customer and financial transaction reporting.

## Users
- Finance analysts
- Operations managers
- Risk analysts
- Data engineering team

## Business questions
1. What is the daily transaction volume and value?
2. Which accounts have the highest failed-transaction rate?
3. Which customers are most active?
4. How do transaction amounts vary by channel and currency?
5. Are source and target record counts reconciled?

## Functional requirements
- Load customer and account files daily
- Load transaction data incrementally
- Retain immutable raw data
- Standardize timestamps, currencies, and status codes
- Reject or quarantine invalid records
- Create fact and dimension tables
- Produce daily KPI aggregates
- Support reruns without creating duplicates

## Non-functional requirements
- Idempotent pipelines
- Parameterized environment configuration
- Logging and audit columns
- Data-quality checks
- Secure secret handling
- Clear runbook and architecture documentation

## MVP success criteria
- All sample datasets load successfully
- Duplicate transaction IDs are prevented
- Invalid records are quarantined
- Daily KPI output matches source totals
- Pipeline can be rerun safely
