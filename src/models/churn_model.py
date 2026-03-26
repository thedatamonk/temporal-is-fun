import pickle
import time

import pandas as pd
from sklearn.ensemble import RandomForestClassifier


def train_churn_model(
    train_df: pd.DataFrame,
    target_col: str = "Churn",
    n_estimators: int = 100,
    random_state: int = 42,
) -> tuple[RandomForestClassifier, dict]:
    X = train_df.drop(columns=[target_col])
    y = train_df[target_col]

    start = time.time()
    model = RandomForestClassifier(
        n_estimators=n_estimators,
        random_state=random_state,
    )
    model.fit(X, y)
    elapsed = time.time() - start

    metadata = {
        "n_estimators": n_estimators,
        "random_state": random_state,
        "n_features": X.shape[1],
        "n_samples": X.shape[0],
        "training_time_seconds": round(elapsed, 2),
    }
    return model, metadata


def serialize(model: RandomForestClassifier) -> bytes:
    return pickle.dumps(model)


def load_model(data: bytes) -> RandomForestClassifier:
    return pickle.loads(data)
