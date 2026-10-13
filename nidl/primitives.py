from collections import defaultdict
import json, math, os, statistics, urllib.request

REGISTRY = {}
CONTRACTS = {}
def primitive(name):
    def wrap(fn):
        REGISTRY[name] = fn
        CONTRACTS[name] = {
            'id': name, 'deterministic': not name.startswith('agent.'),
            'side_effect': name.startswith(('incident.', 'notify.')),
            'timeout_seconds': 60, 'permissions': [name.split('.')[0] + ':execute']
        }
        return fn
    return wrap

@primitive('data.inline@1')
def inline(args, context):
    return {'rows': args['rows']}

@primitive('starrocks.query@1')
def starrocks(args, context):
    templates = context.get('queries', {})
    query_ref = args['query_ref']
    if query_ref not in templates:
        raise ValueError('Unapproved query template')
    if not os.getenv('STARROCKS_HOST'):
        raise RuntimeError('STARROCKS_HOST not configured')
    import pymysql
    sql = templates[query_ref]
    if not sql.lstrip().lower().startswith('select') or ';' in sql:
        raise ValueError('Only single SELECT templates allowed')
    conn = pymysql.connect(host=os.environ['STARROCKS_HOST'], port=int(os.getenv('STARROCKS_PORT','9030')), user=os.getenv('STARROCKS_USER','root'), password=os.getenv('STARROCKS_PASSWORD',''), database=os.getenv('STARROCKS_DATABASE',''), cursorclass=pymysql.cursors.DictCursor, read_timeout=20)
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, {k:v for k,v in args.items() if k != 'query_ref'})
            rows = cursor.fetchmany(int(os.getenv('NIDL_MAX_ROWS','10000')))
            return {'rows': list(rows)}
    finally:
        conn.close()

@primitive('analytics.rules@1')
def rules(args, context):
    operators = {'gt':lambda a,b:a>b, 'gte':lambda a,b:a>=b, 'lt':lambda a,b:a<b, 'lte':lambda a,b:a<=b, 'eq':lambda a,b:a==b, 'ne':lambda a,b:a!=b}
    predicates = args.get('all', [])
    rows = [row for row in args['source'] if all(p['field'] in row and row[p['field']] is not None and operators[p['op']](row[p['field']],p['value']) for p in predicates)]
    return {'rows': rows, 'count':len(rows)}

@primitive('analytics.baseline@1')
def baseline(args, context):
    groups = defaultdict(list)
    for row in args['source']:
        if row.get(args['metric']) is not None:
            groups[row[args['group_by']]].append(row[args['metric']])
    return {'rows':[{'entity':k,'mean':sum(v)/len(v),'samples':len(v)} for k,v in groups.items()]}

@primitive('analytics.anomaly@1')
def anomaly(args, context):
    values = [float(row[args['metric']]) for row in args['source'] if row.get(args['metric']) is not None]
    if len(values) < 2:
        return {'rows': [], 'count': 0}
    mean, deviation = statistics.mean(values), statistics.pstdev(values)
    threshold = float(args.get('z_threshold', 2.5))
    rows = []
    for row in args['source']:
        value = row.get(args['metric'])
        if value is not None:
            score = 0 if deviation == 0 else abs((float(value) - mean) / deviation)
            if score >= threshold:
                rows.append({**row, 'anomaly_score': score})
    return {'rows': rows, 'count': len(rows), 'baseline': {'mean': mean, 'stddev': deviation}}

@primitive('geo.aggregate@1')
def geo_aggregate(args, context):
    groups = defaultdict(list)
    for row in args['source']:
        if row.get(args['metric']) is not None:
            groups[row[args.get('group_by','h3')]].append(row[args['metric']])
    return {'rows':[{'h3':k,'mean':sum(v)/len(v),'count':len(v)} for k,v in groups.items()]}

@primitive('network.impact@1')
def impact(args, context):
    rows = args['cells']
    return {'result':{'affected_cells':len(rows),'estimated_users':sum(int(r.get('estimated_users',0)) for r in rows),'evidence':rows}}

@primitive('incident.correlate@1')
def correlate(args, context):
    events = sorted(args['events'], key=lambda item: item.get('event_time', ''))
    window = int(args.get('window_minutes', 15))
    groups = []
    for event in events:
        key = event.get(args.get('group_by', 'site_id')) or event.get('entity_ref')
        existing = next((group for group in groups if group['key'] == key), None)
        if existing:
            existing['events'].append(event)
        else:
            groups.append({'key': key, 'events': [event], 'window_minutes': window})
    return {'episodes': groups, 'count': len(groups)}

@primitive('incident.upsert@1')
def incident(args, context):
    return context['store'].upsert_incident(args['key'], args['evidence'], context['market'], context.get('tenant', 'default'), int(args.get('severity', 3)))

@primitive('notify.enqueue@1')
def notify(args, context):
    if context.get('dry_run'):
        return {'status':'dry_run'}
    return context['store'].enqueue(args['incident_id'], args.get('channel','inbox'), args.get('destination'), args.get('subscription_id', ''), args.get('fingerprint'))

@primitive('agent.investigate@1')
def agent(args, context):
    endpoint = os.getenv('NIDL_AGENT_ENDPOINT')
    if not endpoint:
        return {'status':'not_configured','hypotheses':[]}
    if not endpoint.startswith('https://'):
        raise ValueError('Agent endpoint must use HTTPS')
    body = json.dumps({'evidence':args['evidence'],'instruction':args.get('instruction','Investigate evidence')}).encode()
    req = urllib.request.Request(endpoint, data=body, headers={'Content-Type':'application/json','Authorization':'Bearer '+os.getenv('NIDL_AGENT_TOKEN','')})
    with urllib.request.urlopen(req,timeout=15) as response:
        result = json.load(response)
    if not isinstance(result, dict) or not isinstance(result.get('hypotheses', []), list):
        raise ValueError('Agent response must be an object with a hypotheses list')
    return result
