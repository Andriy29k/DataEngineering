import numpy as np
import pandas as pd

data = {
    'A': np.random.randint(1, 50, size = 10),
    'B': np.random.randint(1, 50, size = 10)
}

df = pd.DataFrame(data)

df['C'] = df['A'] * df['B']

df

