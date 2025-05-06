import pandas as pd
import numpy as np
import torch
from pytorch_tabnet.tab_model import TabNetRegressor
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_squared_error, r2_score

# 1) LOAD & PREPROCESS
df = pd.read_csv("asp_data.csv", parse_dates=["Journey Date"])
df = (
    df.drop(columns=["Service Number", "Journey Day of Week"])
      .rename(columns={
          "Journey Date":       "journey_date",
          "Seat Number":        "seat_number",
          "Booking Lead Time":  "lead_time_hours",
          "Holiday Type":       "holiday_type",
          "Demand Day":         "demand_day",
          "Day Before Holiday": "days_before_holiday",
          "Lead Time Bin":      "lead_time_bin",
          "Seat Availability":  "seat_availability",
          "ASP":                "asp",
          "Journey Hour":       "hour",
      })
      .sort_values("journey_date")
      .reset_index(drop=True)
)

# integer time index (days since first date)
df["time_idx"] = (df["journey_date"] - df["journey_date"].min()).dt.days

# 2) FEATURE ENGINEERING
# cyclical encodings
df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
df["dow"]     = df["journey_date"].dt.dayofweek
df["dow_sin"] = np.sin(2 * np.pi * df["dow"] / 7)
df["dow_cos"] = np.cos(2 * np.pi * df["dow"] / 7)
df["month"]   = df["journey_date"].dt.month
df["mon_sin"] = np.sin(2 * np.pi * (df["month"] - 1) / 12)
df["mon_cos"] = np.cos(2 * np.pi * (df["month"] - 1) / 12)

# occupancy ratio
CAPACITY = 44
df["occupancy_ratio"] = (CAPACITY - df["seat_availability"]) / CAPACITY

# lags & rolling means
for lag in [1, 7]:
    df[f"asp_lag_{lag}"] = df["asp"].shift(lag)
for w in [3, 7]:
    df[f"asp_roll_{w}"] = df["asp"].rolling(window=w, min_periods=1).mean()

df.fillna(method="bfill", inplace=True)
df.fillna(method="ffill", inplace=True)

# drop original date, hour, cyclical intermediates
df = df.drop(columns=["journey_date","hour","dow","month"])

# 3) TRAIN/TEST SPLIT (80% train chronological)
cut = int(len(df) * 0.8)
train_df = df.iloc[:cut].reset_index(drop=True)
test_df  = df.iloc[cut:].reset_index(drop=True)

# 4) LABEL ENCODE CATEGORICALS
TARGET = "asp"
cat_cols = [
    "seat_number","holiday_type","demand_day",
    "days_before_holiday","lead_time_bin"
]
for col in cat_cols:
    le = LabelEncoder()
    train_df[col] = le.fit_transform(train_df[col].astype(str))
    test_df[col]  = le.transform(test_df[col].astype(str))

features = [c for c in train_df.columns if c != TARGET]

X_train = train_df[features].values
y_train = train_df[TARGET].values.reshape(-1, 1)
X_test  = test_df[features].values
y_test  = test_df[TARGET].values.reshape(-1, 1)

# 5) INITIALIZE TABNET REGRESSOR
tabnet = TabNetRegressor(
    n_d=32, n_a=32, n_steps=5,
    gamma=1.5, lambda_sparse=1e-4,
    optimizer_fn=torch.optim.Adam,
    optimizer_params={'lr': 2e-2},
    mask_type="sparsemax",
    seed=42,
)

# 6) TRAIN WITH EARLY STOPPING
tabnet.fit(
    X_train, y_train,
    eval_set=[(X_train, y_train), (X_test, y_test)],
    eval_name=["train", "test"],
    eval_metric=["rmse"],
    max_epochs=100,
    patience=20,
    batch_size=2048,
    virtual_batch_size=256,
    num_workers=0,
    drop_last=False
)

# 7) FINAL EVALUATION
preds = tabnet.predict(X_test).flatten()
rmse = np.sqrt(mean_squared_error(y_test, preds))
r2   = r2_score(y_test, preds)

print(f"\nFinal Test RMSE: {rmse:.3f}")
print(f"Final Test  R² : {r2:.3f}")

tabnet.save_model("tabnet_asp_model.zip")

loaded = TabNetRegressor()
loaded.load_model("tabnet_asp_model.zip.zip")  # note double .zip
preds = loaded.predict(X_test).flatten()

results = test_df.copy()
results["Actual ASP"]    = results["asp"]
results["Predicted ASP"] = preds

original = pd.read_csv("asp_data.csv", parse_dates=["Journey Date"])
original = original.sort_values("Journey Date").reset_index(drop=True)
orig_test = original.iloc[cut:].reset_index(drop=True)
orig_test["Predicted ASP"] = preds
final = orig_test[[
    "Journey Date", "Seat Number", "ASP", "Predicted ASP"
]].rename(columns={"ASP":"Actual ASP"})
final.to_csv("asp_test_predictions.csv", index=False)
