# coding: utf-8
import pandas as pd

import dask.dataframe as dd


def test_repr():
    df = pd.DataFrame({'x': list(range(100))})
    ddf = dd.from_pandas(df, 3)

    for x in [ddf, ddf.index, ddf.x]:
        assert type(x).__name__ in repr(x)
        assert str(x.npartitions) in repr(x)


def test_dataframe_format():
    df = pd.DataFrame({'A': [1, 2, 3, 4, 5, 6, 7, 8],
                       'B': list('ABCDEFGH'),
                       'C': pd.Categorical(list('AAABBBCC'))})
    ddf = dd.from_pandas(df, 3)
    exp = ("Dask DataFrame Structure:\n"
           "                   A       B         C\n"
           "npartitions=3                         \n"
           "0              int64  object  category\n"
           "3                ...     ...       ...\n"
           "6                ...     ...       ...\n"
           "7                ...     ...       ...")
    assert repr(ddf) == exp
    assert str(ddf) == exp

    exp = ("                   A       B         C\n"
           "npartitions=3                         \n"
           "0              int64  object  category\n"
           "3                ...     ...       ...\n"
           "6                ...     ...       ...\n"
           "7                ...     ...       ...")
    assert ddf.to_string() == exp

    exp_table = """<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>A</th>
      <th>B</th>
      <th>C</th>
    </tr>
    <tr>
      <th>npartitions=3</th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>int64</td>
      <td>object</td>
      <td>category</td>
    </tr>
    <tr>
      <th>3</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>6</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>7</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
  </tbody>
</table>"""

    exp = """<div><strong>Dask DataFrame Structure:</strong></div>
{exp_table}""".format(exp_table=exp_table)
    assert ddf.to_html() == exp

    # table is boxed with div
    exp = """<div><strong>Dask DataFrame Structure:</strong></div>
<div>
{exp_table}
</div>""".format(exp_table=exp_table)
    assert ddf._repr_html_() == exp


def test_dataframe_format_with_index():
    df = pd.DataFrame({'A': [1, 2, 3, 4, 5, 6, 7, 8],
                       'B': list('ABCDEFGH'),
                       'C': pd.Categorical(list('AAABBBCC'))},
                      index=list('ABCDEFGH'))
    ddf = dd.from_pandas(df, 3)
    exp = ("Dask DataFrame Structure:\n"
           "                   A       B         C\n"
           "npartitions=3                         \n"
           "A              int64  object  category\n"
           "D                ...     ...       ...\n"
           "G                ...     ...       ...\n"
           "H                ...     ...       ...")
    assert repr(ddf) == exp
    assert str(ddf) == exp

    exp_table = """<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>A</th>
      <th>B</th>
      <th>C</th>
    </tr>
    <tr>
      <th>npartitions=3</th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>A</th>
      <td>int64</td>
      <td>object</td>
      <td>category</td>
    </tr>
    <tr>
      <th>D</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>G</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>H</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
  </tbody>
</table>"""

    exp = """<div><strong>Dask DataFrame Structure:</strong></div>
{exp_table}""".format(exp_table=exp_table)
    assert ddf.to_html() == exp

    # table is boxed with div
    exp = """<div><strong>Dask DataFrame Structure:</strong></div>
<div>
{exp_table}
</div>""".format(exp_table=exp_table)
    assert ddf._repr_html_() == exp


def test_dataframe_format_unknown_divisions():
    df = pd.DataFrame({'A': [1, 2, 3, 4, 5, 6, 7, 8],
                       'B': list('ABCDEFGH'),
                       'C': pd.Categorical(list('AAABBBCC'))})
    ddf = dd.from_pandas(df, 3)
    ddf = ddf.clear_divisions()
    assert not ddf.known_divisions

    exp = ("Dask DataFrame Structure:\n"
           "                   A       B         C\n"
           "npartitions=3                         \n"
           "None           int64  object  category\n"
           "None             ...     ...       ...\n"
           "None             ...     ...       ...\n"
           "None             ...     ...       ...")
    assert repr(ddf) == exp
    assert str(ddf) == exp

    exp = ("                   A       B         C\n"
           "npartitions=3                         \n"
           "None           int64  object  category\n"
           "None             ...     ...       ...\n"
           "None             ...     ...       ...\n"
           "None             ...     ...       ...")
    assert ddf.to_string() == exp

    exp_table = """<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>A</th>
      <th>B</th>
      <th>C</th>
    </tr>
    <tr>
      <th>npartitions=3</th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>None</th>
      <td>int64</td>
      <td>object</td>
      <td>category</td>
    </tr>
    <tr>
      <th>None</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>None</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>None</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
  </tbody>
</table>"""

    exp = """<div><strong>Dask DataFrame Structure:</strong></div>
{exp_table}""".format(exp_table=exp_table)
    assert ddf.to_html() == exp

    # table is boxed with div
    exp = """<div><strong>Dask DataFrame Structure:</strong></div>
<div>
{exp_table}
</div>""".format(exp_table=exp_table)
    assert ddf._repr_html_() == exp


def test_dataframe_format_long():
    df = pd.DataFrame({'A': [1, 2, 3, 4, 5, 6, 7, 8] * 10,
                       'B': list('ABCDEFGH') * 10,
                       'C': pd.Categorical(list('AAABBBCC') * 10)})
    ddf = dd.from_pandas(df, 10)
    exp = ('Dask DataFrame Structure:\n'
           '                    A       B         C\n'
           'npartitions=10                         \n'
           '0               int64  object  category\n'
           '8                 ...     ...       ...\n'
           '...               ...     ...       ...\n'
           '72                ...     ...       ...\n'
           '79                ...     ...       ...')
    assert repr(ddf) == exp
    assert str(ddf) == exp

    exp = ("                    A       B         C\n"
           "npartitions=10                         \n"
           "0               int64  object  category\n"
           "8                 ...     ...       ...\n"
           "...               ...     ...       ...\n"
           "72                ...     ...       ...\n"
           "79                ...     ...       ...")
    assert ddf.to_string() == exp

    exp_table = """<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>A</th>
      <th>B</th>
      <th>C</th>
    </tr>
    <tr>
      <th>npartitions=10</th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>int64</td>
      <td>object</td>
      <td>category</td>
    </tr>
    <tr>
      <th>8</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>72</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>79</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
  </tbody>
</table>"""

    exp = """<div><strong>Dask DataFrame Structure:</strong></div>
{exp_table}""".format(exp_table=exp_table)
    assert ddf.to_html() == exp

    # table is boxed with div
    exp = u"""<div><strong>Dask DataFrame Structure:</strong></div>
<div>
{exp_table}
</div>""".format(exp_table=exp_table)
    assert ddf._repr_html_() == exp


def test_series_format():
    s = pd.Series([1, 2, 3, 4, 5, 6, 7, 8],
                  index=list('ABCDEFGH'))
    ds = dd.from_pandas(s, 3)
    exp = """Dask Series Structure:
npartitions=3
A    int64
D      ...
G      ...
H      ...
dtype: int64"""
    assert repr(ds) == exp
    assert str(ds) == exp

    exp = """npartitions=3
A    int64
D      ...
G      ...
H      ..."""
    assert ds.to_string() == exp

    s = pd.Series([1, 2, 3, 4, 5, 6, 7, 8],
                  index=list('ABCDEFGH'), name='XXX')
    ds = dd.from_pandas(s, 3)
    exp = """Dask Series Structure:
npartitions=3
A    int64
D      ...
G      ...
H      ...
Name: XXX, dtype: int64"""
    assert repr(ds) == exp
    assert str(ds) == exp


def test_series_format_long():
    s = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10] * 10,
                  index=list('ABCDEFGHIJ') * 10)
    ds = dd.from_pandas(s, 10)
    exp = ("Dask Series Structure:\nnpartitions=10\nA    int64\nB      ...\n"
           "     ...  \nJ      ...\nJ      ...\ndtype: int64")
    assert repr(ds) == exp
    assert str(ds) == exp

    exp = "npartitions=10\nA    int64\nB      ...\n     ...  \nJ      ...\nJ      ..."
    assert ds.to_string() == exp


def test_index_format():
    s = pd.Series([1, 2, 3, 4, 5, 6, 7, 8],
                  index=list('ABCDEFGH'))
    ds = dd.from_pandas(s, 3)
    exp = """Dask Index Structure:
npartitions=3
A    object
D       ...
G       ...
H       ...
dtype: object"""
    assert repr(ds.index) == exp
    assert str(ds.index) == exp

    s = pd.Series([1, 2, 3, 4, 5, 6, 7, 8],
                  index=pd.CategoricalIndex([1, 2, 3, 4, 5, 6, 7, 8], name='YYY'))
    ds = dd.from_pandas(s, 3)
    exp = """Dask Index Structure:
npartitions=3
1    category
4         ...
7         ...
8         ...
Name: YYY, dtype: category"""
    assert repr(ds.index) == exp
    assert str(ds.index) == exp