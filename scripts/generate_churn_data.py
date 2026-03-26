import csv
import random

random.seed(42)

GENDERS = ["Male", "Female"]
YES_NO = ["Yes", "No"]
INTERNET = ["DSL", "Fiber optic", "No"]
CONTRACTS = ["Month-to-month", "One year", "Two year"]

rows = []
for i in range(1, 1001):
    gender = random.choice(GENDERS)
    senior = random.choice([0, 1])
    partner = random.choice(YES_NO)
    dependents = random.choice(YES_NO)
    tenure = random.randint(1, 72)
    internet = random.choice(INTERNET)
    phone = "No" if internet == "No" and random.random() < 0.3 else "Yes"
    contract = random.choice(CONTRACTS)
    monthly = round(random.uniform(18.0, 100.0), 2)
    total = round(monthly * tenure, 2) if random.random() > 0.05 else ""  # 5% missing

    # Churn more likely for: month-to-month, fiber optic, short tenure
    churn_score = 0
    if contract == "Month-to-month":
        churn_score += 2
    if internet == "Fiber optic":
        churn_score += 1
    if tenure < 12:
        churn_score += 2
    churn = "Yes" if random.random() < (churn_score / 8) else "No"

    rows.append([i, gender, senior, partner, dependents, tenure, phone,
                 internet, contract, monthly, total, churn])

with open("data/churn_1000.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["customerID", "gender", "SeniorCitizen", "Partner",
                     "Dependents", "tenure", "PhoneService", "InternetService",
                     "Contract", "MonthlyCharges", "TotalCharges", "Churn"])
    writer.writerows(rows)

print(f"Generated {len(rows)} rows -> data/churn_1000.csv")
