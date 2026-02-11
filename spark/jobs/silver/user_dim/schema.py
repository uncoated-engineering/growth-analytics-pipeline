"""
Silver user dimension output columns:
    user_id (INT)
    signup_date (DATE)
    company_size (STRING)
    industry (STRING)
    current_plan (STRING) - Joined from conversions, defaults to 'free'
"""

USER_DIM_COLUMNS = [
    "user_id",
    "signup_date",
    "company_size",
    "industry",
    "current_plan",
]
