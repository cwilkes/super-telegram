import boto3
import sys
import decimal
import datetime


TBL_CONFIG = 'config_values'
TBL_TAGS = 'tags'


def get_all_keys_for_env(env):
    items = _tbl_config().scan(ScanFilter=_mk_attr_dict('env', env))
    return set(_['name'] for _ in items['Items'])


def get_values_all_latest_versions(env, keys, throw_exception_if_missing=True):
    keys_and_versions = list()
    for key in keys:
        version = _next_version(env, key)-1
        if version == -1:
            if throw_exception_if_missing:
                raise Exception('Asked for "%s" in env %s but not found' % (key, env))
            else:
                continue
        keys_and_versions.append((key, version))
    ret = get_values(env, keys_and_versions)
    ret2 = dict()
    for k, v in ret.items():
        ret2[k] = (0, v)
    return ret2


def get_values(env, keys_and_versions):
    ret = dict()
    table = _tbl_config()
    for name, version in keys_and_versions:
        key_conditions = _mk_primary_key(name, env)
        key_conditions.update(_mk_attr_dict('version', version))
        tq = table.query(
            KeyConditions=key_conditions,
            Limit=1
        )
        try:
            ret[name] = tq['Items'][0]['value']
        except:
            raise Exception('In %s do not have a version %d for "%s", do have versions %s'
                            % (env, version, name, get_versions(name, env)))
    return _fix_tree(ret)


def get_versions_and_values(env, name, only_max_version=False, table_config=True):
    if type(name) == str:
        key_filter=_mk_attr_dict('name_env', _config_primary(name, env))
        table = _tbl_config() if table_config else _tbl_tags()
        ret = list()
        items = table.query(
            KeyConditions=key_filter,
            ScanIndexForward=False,
            Limit=1 if only_max_version else sys.maxsize
        )['Items']
        for v in items:
            print('v', v)
            version = v['version']
            value = v['value'] if table_config else v['values']
            ret.append((int(version), _fix_tree(value)))
        return {name: ret}
    else:
        ret = dict()
        for n2 in name:
            ret.update(get_versions_and_values(env, n2, only_max_version, table_config))
        return ret


def get_versions(env, name, only_max_version=False, table_config=True):
    # could copy get_versions_and_values and just ask for AttributesToGet=['version',],
    vv = get_versions_and_values(env, name, only_max_version, table_config)
    return [_[0] for _ in vv[name]]


def add_config_value(env, name, value):
    version = _next_version(env, name)
    if type(value) == float:
        value = decimal.Decimal(value)
    item = dict(
        name_env=_config_primary(name, env),
        name=name,
        env=env,
        value=value,
        version=version,
        date=datetime.datetime.now().isoformat()
    )
    res=_tbl_config().put_item(Item=item, ReturnValues='ALL_OLD')
    # res should have nothing in it
    return version


def list_tags(env):
    items = _tbl_tags().scan(ScanFilter=_mk_attr_dict('env', 'dev'), AttributesToGet=['name', ])['Items']
    return set(_['name'] for _ in items)


def get_tag(tag_name, env, version):
    versions = get_versions_and_values(env, tag_name, table_config=False)[tag_name]
    for this_version, val in versions:
        if this_version == version:
            keys_and_values = [(_['key'], _['version']) for _ in val]
            return get_values(env, keys_and_values)
    raise Exception('No tag %s in %s with version %s, do have %s' % (tag_name, env, version, [_[0] for _ in versions]))


def create_tag(tag_name, env, config_names_and_versions):
    _check_collection(config_names_and_versions, 'config_names_and_versions')
    tag_keys = _make_tag_keys(env, config_names_and_versions)
    version = _next_version(env, tag_name, False)
    # date will be in local tz, need to convert to UTC
    item = dict(
        name_env=_config_primary(tag_name, env),
        name=tag_name,
        env=env,
        version=version,
        values=tag_keys,
        date=datetime.datetime.now().isoformat()
    )
    res = _tbl_tags().put_item(Item=item, ReturnValues='ALL_OLD')
    return version


def _make_tag_keys(env, config_names_and_versions):
    tag_keys = list()
    for key in config_names_and_versions:
        if type(key) == str:
            version = _next_version(env, key) - 1
            if version == -1:
                raise Exception('Do not have a value for "%s" in env %s' % (key, env))
        else:
            key, version = key
        tag_keys.append(dict(key=key, version=version))
    return tag_keys


def setup_tables():
    dynamodb = boto3.resource('dynamodb')
    res1 = dynamodb.create_table(
        TableName=TBL_CONFIG,
        AttributeDefinitions=_attr_defs(('name_env', 'S'), ('version', 'N')),
        KeySchema=_schema_def('name_env', 'version'),
        ProvisionedThroughput=dict(ReadCapacityUnits=10, WriteCapacityUnits=10,)
    )
    res2 = dynamodb.create_table(
        TableName=TBL_TAGS,
        AttributeDefinitions=_attr_defs(('name_env', 'S'), ('version', 'N')),
        KeySchema=_schema_def('name_env', 'version'),
        ProvisionedThroughput=dict(ReadCapacityUnits=10, WriteCapacityUnits=10,)
    )
    return res1, res2


def _check_collection(val, value_name=None):
    try:
        len(val)
    except:
        raise Exception('%s must be a list or tuple' % (value_name, ))


def _get_table(table_name):
    dynamodb = boto3.resource('dynamodb')
    return dynamodb.Table(table_name)


def _tbl_config():
    return _get_table(TBL_CONFIG)


def _tbl_tags():
    return _get_table(TBL_TAGS)


def _config_primary(name, env):
    if name is None or env is None:
        raise Exception('Name (%s) and Env (%s) cannot be None' (name, env))
    if type(name) != str or type(env) != str:
        raise Exception('Name (%s) and Env (%s) must be both strings' % (type(name), type(env)))
    return '%s_%s' % (name, env)


def _mk_attr_dict(name, value):
    return {name: dict(AttributeValueList=[value, ], ComparisonOperator='EQ')}


def _attr_defs(*name_attr_types):
    return [dict(AttributeName=key, AttributeType=value) for key, value in name_attr_types]


def _schema_def(*hash_and_range):
    ret = list()
    ret.append(dict(AttributeName=hash_and_range[0], KeyType='HASH'))
    if len(hash_and_range) == 2:
        ret.append(dict(AttributeName=hash_and_range[1], KeyType='RANGE'))
    return ret


def _fix_decimal_value(v):
    if v == int(v):
        return int(v)
    else:
        return float(v)


def _is_decimal(v):
    return type(v) == decimal.Decimal


def _fix_tree(tree):
    if type(tree) == dict:
        return dict((k,_fix_tree(v)) for k, v in tree.items())
    elif type(tree) == list:
        return [_fix_tree(_) for _ in tree]
    else:
        return _fix_decimal_value(tree) if _is_decimal(tree) else tree


def _mk_primary_key(name, env):
    return _mk_attr_dict('name_env', _config_primary(name, env))


def _next_version(env, name, table_config=True):
    versions = get_versions(env, name, True, table_config)
    if versions:
        return versions[0]+1
    else:
        return 0

