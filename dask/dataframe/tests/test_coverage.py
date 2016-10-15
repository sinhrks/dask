
import pandas as pd
import dask.dataframe as dd


def test_Dataframe():

    # can get current difference:

    # [x for x in sorted(list(set(dir(pd.Series)) -
    #  set(dir(dd.Series)))) if not x.startswith('_')]

    whitelist = ['T', 'abs', 'add_prefix', 'add_suffix', 'align', 'all',
                 'any', 'as_blocks', 'as_matrix', 'asfreq', 'at', 'at_time',
                 'axes', 'between_time', 'bfill', 'blocks', 'bool', 'boxplot',
                 'clip', 'clip_lower', 'clip_upper', 'combine', 'combineAdd',
                 'combineMult', 'combine_first', 'compound', 'consolidate',
                 'convert_objects', 'copy', 'corrwith', 'diff', 'divide',
                 'dot', 'duplicated', 'empty', 'equals', 'ewm', 'expanding',
                 'ffill', 'filter', 'first', 'first_valid_index', 'from_csv',
                 'from_dict', 'from_items', 'from_records', 'ftypes', 'get',
                 'get_value', 'get_values', 'hist', 'iat', 'icol',
                 'iget_value', 'iloc', 'insert', 'interpolate', 'irow',
                 'is_copy', 'isin', 'items', 'iteritems', 'iterkv', 'ix',
                 'keys', 'kurt', 'kurtosis', 'last', 'last_valid_index',
                 'lookup', 'mad', 'median', 'memory_usage', 'mode', 'multiply',
                 'nsmallest', 'pct_change', 'pivot', 'pivot_table', 'plot',
                 'pop', 'prod', 'product', 'rank', 'reindex', 'reindex_axis',
                 'reindex_like', 'rename_axis', 'reorder_levels', 'replace',
                 'resample', 'select', 'sem', 'set_axis', 'set_value',
                 'shape', 'shift', 'skew', 'slice_shift', 'sort', 'sort_index',
                 'sort_values', 'sortlevel', 'squeeze', 'stack', 'style',
                 'subtract', 'swapaxes', 'swaplevel', 'take', 'to_clipboard',
                 'to_dense', 'to_dict', 'to_excel', 'to_gbq', 'to_html',
                 'to_json', 'to_latex', 'to_msgpack', 'to_panel', 'to_period',
                 'to_pickle', 'to_records', 'to_sparse', 'to_sql', 'to_stata',
                 'to_string', 'to_wide', 'to_xarray', 'transpose', 'truncate',
                 'tshift', 'tz_convert', 'tz_localize', 'unstack', 'update',
                 'values', 'xs']

    current = dir(dd.DataFrame)
    expected = dir(pd.DataFrame)

    for exp in expected:

        if exp.startswith('_') or exp in whitelist:
            continue

        if exp not in current:
            msg = 'method/property {0} is not supported'
            raise AssertionError(msg.format(exp))

    for white in whitelist:
        if white in current:
            msg = 'method/property {0} is supported already'
            raise AssertionError(msg.format(exp))


def test_Series():

    whitelist = ['T', 'abs', 'add_prefix', 'add_suffix', 'align', 'all',
                 'any', 'argmax', 'argmin', 'argsort', 'as_blocks',
                 'as_matrix', 'asfreq', 'asof', 'at', 'at_time', 'autocorr',
                 'axes', 'base', 'between_time', 'bfill', 'blocks', 'bool',
                 'clip_lower', 'clip_upper', 'combine', 'combine_first',
                 'compound', 'compress', 'consolidate', 'convert_objects',
                 'copy', 'data', 'diff', 'divide', 'dot', 'drop', 'dtypes',
                 'duplicated', 'empty', 'equals', 'ewm', 'expanding',
                 'factorize', 'ffill', 'filter', 'first', 'first_valid_index',
                 'flags', 'from_array', 'from_csv', 'ftype', 'ftypes',
                 'get', 'get_dtype_counts', 'get_ftype_counts', 'get_value',
                 'get_values', 'hasnans', 'hist', 'iat', 'iget', 'iget_value',
                 'iloc', 'imag', 'interpolate', 'irow', 'is_copy',
                 'is_time_series', 'is_unique', 'item', 'items', 'itemsize',
                 'iterkv', 'ix', 'keys', 'kurt', 'kurtosis', 'last',
                 'last_valid_index', 'mad', 'median', 'memory_usage', 'mode',
                 'multiply', 'nonzero', 'nsmallest', 'order', 'pct_change',
                 'plot', 'pop', 'prod', 'product', 'ptp', 'put', 'rank',
                 'ravel', 'real', 'reindex', 'reindex_axis', 'reindex_like',
                 'rename', 'rename_axis', 'reorder_levels', 'repeat',
                 'replace', 'reset_index', 'reshape', 'searchsorted', 'select',
                 'sem', 'set_axis', 'set_value', 'shape', 'shift', 'skew',
                 'slice_shift', 'sort', 'sort_index', 'sort_values',
                 'sortlevel', 'squeeze', 'strides', 'subtract', 'swapaxes',
                 'swaplevel', 'take', 'to_clipboard', 'to_dense', 'to_dict',
                 'to_json', 'to_msgpack', 'to_period', 'to_pickle',
                 'to_sparse', 'to_sql', 'to_string', 'to_xarray', 'tolist',
                 'transpose', 'truncate', 'tshift', 'tz_convert',
                 'tz_localize', 'unstack', 'update', 'valid', 'values', 'view',
                 'xs']

    current = dir(dd.Series)
    expected = dir(pd.Series)

    for exp in expected:

        if exp.startswith('_') or exp in whitelist:
            continue

        if exp not in current:
            msg = 'method/property {0} is not supported'
            raise AssertionError(msg.format(exp))

    for white in whitelist:
        if white in current:
            msg = 'method/property {0} is supported already'
            raise AssertionError(msg.format(exp))
