import pandas as pd
import matplotlib.pyplot as plt

# Read the TSV data
df = pd.read_csv('results.csv', sep='\t', index_col=False)

# Group by flags and calculate means
grouped = df.groupby('flags').agg({
    'build_time': ['median', 'mean', 'std'],
    'runtime': ['median', 'mean', 'std']
})

print(grouped.sort_values(('runtime', 'median')))
