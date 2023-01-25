# Light Pandas

## Brief

**Pandas** is a great tools for data analysis.<br>
But the execution speed is very slow. So I try to provide a light library.
The properties as below:

- **Light Pandas** only provide part of functions which provided by pandas.
- The code can compatible pandas. That's mean the code should execute OK if replaced light pandas by pandas.
- **Light Pandas** execution speed is faster than pandas.
- The cell type only support str.
- TBD

## Install
```
pip install light-pandas
```

## Example

- Create an empty DataFrame which contained two columns.

```
import lightpandas as pd
df = lpd.DataFrame(columns=('item1', 'item2'))
```

- Filter in the rows which the column "item4" is littler than "t4" (string compare).

```
df.loc[df['item1'] < 't4']
```
