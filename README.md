# Fraud Detection Project

Quick project to find fraudulent users given information about said users and their transaction history.
			
The Task (all items completed):
								
1. Write an ETL script in Python to load and store the data in a local PostgreSQL database. 

2. Write a query to identify users whose first transaction was a successful card payment over $10 USD. (Note: Use the rate from latest timestamp before the transaction timestamp)

3. The is_fraudster column in the users table indicates individuals who have been marked as fraudsters. Goal is to use the data to design a system (rules, ML or mathematical/statistical) to identify fraudsters and conduct an action {LOCK_USER, ALERT_AGENT, BOTH} based on the result. It is important to consider consequential impact on all domains (e.g. customers, bank, etc.).

a) Explore the data for fraudsters and regular users. What are your observations? What would be your features for identifying fraudsters? Explain your reasoning.

b) Create a model which identifies fraudsters. Assess the quality of it and explain.

c) How will you utilise this model to catch fraudsters? What will be the resulting action (LOCK USER, ALERT, BOTH) and what is the impact?

d) Write a simple algorithm which implements this model. Your final code should accept a user_id and yield your suggested action as a string (e.g. patrol(user_id_1) = 'BOTH').
