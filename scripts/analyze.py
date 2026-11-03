import csv
import json
import math
import statistics
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from analysis import fit_usl, select_capacity

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'results'
FIG=ROOT/'figures'


def read(path):return json.loads(path.read_text(encoding='utf-8'))

def fmt(value,places=3):
    return '—' if value is None or not math.isfinite(float(value)) else f'{value:.{places}f}'


def mean_vector(vector):
    values=[float(v['value'][1]) for v in vector if math.isfinite(float(v['value'][1]))]
    return statistics.mean(values) if values else None


def plot_profile(n,run,prom):
    rows=run['seconds'];times=[r['second'] for r in rows]
    fig,axes=plt.subplots(2,1,figsize=(11,7),sharex=True)
    axes[0].step(times,[r['target_rps'] for r in rows],where='post',label='Target')
    axes[0].plot(times,[r['achieved_rps'] for r in rows],label='2xx completed/s')
    axes[0].set_ylabel('Requests/s');axes[0].legend()
    axes[1].plot(times,[r['p99_ms'] for r in rows],label='Client p99 (scheduled)')
    for series in prom['ranges']['server_p99']:
        axes[1].plot([float(t)-run['start_epoch'] for t,v in series['values']],
                     [float(v)*1000 for t,v in series['values']],label='Server p99 (15 s window)')
    axes[1].set_ylabel('Latency, ms');axes[1].set_xlabel('Seconds from run start');axes[1].legend()
    for ax in axes:ax.grid(alpha=.25)
    fig.suptitle(f'N={n}');fig.tight_layout();fig.savefig(FIG/f'profile-{n}.png',dpi=160);plt.close(fig)
    fig,axes=plt.subplots(3,1,figsize=(11,9),sharex=True)
    for ax,key,label in zip(axes,['requests_by_instance','cpu_by_instance','inflight_by_instance'],
                            ['Successful requests/s','CPU cores used','Requests in progress']):
        for series in prom['ranges'][key]:
            ax.plot([float(t)-run['start_epoch'] for t,v in series['values']],
                     [float(v) for t,v in series['values']],label=series['metric'].get('instance','web'))
        ax.set_ylabel(label);ax.legend(fontsize=8);ax.grid(alpha=.25)
    axes[-1].set_xlabel('Seconds from run start');fig.suptitle(f'Each web instance, N={n}')
    fig.tight_layout();fig.savefig(FIG/f'instances-{n}.png',dpi=160);plt.close(fig)


def main():
    selected=read(OUT/'selected-series.json')
    folder=OUT/selected['folder'];g=selected['g'];FIG.mkdir(exist_ok=True)
    capacities=[];runs={};proms={}
    for n in (1,2,3,4):
        runs[n]=read(folder/f'run-{n}.json');proms[n]=read(folder/f'prometheus-{n}.json')
        capacities.append(select_capacity(runs[n],g,n))
        seconds=runs[n]['seconds']
        with (folder/f'seconds-{n}.csv').open('w',newline='',encoding='utf-8') as out:
            writer=csv.DictWriter(out,fieldnames=list(seconds[0]));writer.writeheader();writer.writerows(seconds)
    fit=fit_usl(capacities);(OUT/'analysis.json').write_text(json.dumps(dict(capacities=capacities,fit=fit),indent=2))
    for n in (1,4):plot_profile(n,runs[n],proms[n])
    fig,ax=plt.subplots(figsize=(10,6))
    for n in (1,4):
        by_instance={}
        for step in proms[n]['steps']:
            for s in step['throttling']:
                key=s['metric'].get('instance','web')
                by_instance.setdefault(key,[]).append((step['rate'],float(s['value'][1])))
        for instance,pairs in by_instance.items():
            ax.plot([p[0] for p in pairs],[p[1] for p in pairs],marker='o',label=f'N={n}, {instance}')
    ax.set_xlabel('Target requests/s');ax.set_ylabel('Δnr_throttled / Δnr_periods');ax.grid(alpha=.25);ax.legend(fontsize=8)
    fig.tight_layout();fig.savefig(FIG/'throttling.png',dpi=160);plt.close(fig)
    if fit['valid']:
        fig,ax=plt.subplots(figsize=(9,6))
        ns=[1+i*.05 for i in range(181)]
        lam,sigma,kappa=fit['lambda_'],fit['sigma'],fit['kappa']
        ax.plot(ns,[lam*n/(1+sigma*(n-1)+kappa*n*(n-1)) for n in ns],label='USL fit')
        ax.plot(ns,[lam*n for n in ns],linestyle='--',label='Ideal λN')
        for point in capacities:
            limited=point['generator_limited']
            ax.errorbar(point['replicas'],point['x'],yerr=point['std'],fmt='x' if limited else 'o',
                        label=f'N={point["replicas"]}'+(' excluded: generator limit' if limited else ''))
        ax.set_xlabel('N');ax.set_ylabel('Successful requests/s');ax.grid(alpha=.25);ax.legend(fontsize=8)
        fig.tight_layout();fig.savefig(FIG/'usl.png',dpi=160);plt.close(fig)
    lines=['# Числові результати лабораторної роботи 2','',
           f'Межа генератора G = {fmt(g)} rps. Ліміт web = {selected["web_cpus"]} CPU; пам’ять = 1 ГБ; пул = 15.',
           '', '| N | X(N), rps | Стандартне відхилення, rps | Сходинка, rps | p99 клієнта, мс | p99 сервера, мс | Не 2xx, % | Помилки з’єднання | CPU на web, ядер | Запити в обробці на web | Обмежено генератором |',
           '|---|---|---|---|---|---|---|---|---|---|---|']
    for point in capacities:
        n=point['replicas'];best=point['best']
        server=next(s for s in proms[n]['steps'] if s['rate']==point['step_rate'])
        quant=server['p99'];server_value=('> '+fmt(quant['value']*1000)) if quant['overflow'] else fmt(None if quant['value'] is None else quant['value']*1000)
        lines.append(f'| {n} | {fmt(point["x"])} | {fmt(point["std"])} | {point["step_rate"]} | {fmt(best["p99_ms"])} | {server_value} | '+
                     f'{fmt(None if best["non2xx_fraction"] is None else 100*best["non2xx_fraction"])} | {best["connection_errors"]} | '+
                     f'{fmt(mean_vector(server["cpu"]))} | {fmt(mean_vector(server["inflight"]))} | {"так" if point["generator_limited"] else "ні"} |')
    lines+=['','## p99 за сходинками одного екземпляра','',
            '| Ціль, rps | p99 клієнта, мс | p99 сервера, мс | Бакет, мс | Максимальна похибка, мс |',
            '|---|---|---|---|---|']
    for step,server in zip(runs[1]['steps'],proms[1]['steps']):
        q=server['p99'];value=q['value'];bound=q['error_bound']
        bucket=f'({fmt(None if q["lower"] is None else q["lower"]*1000)}, {fmt(None if q["upper"] is None else q["upper"]*1000)}]'
        if q['overflow']:bucket='> 1000';error='не обмежена зверху';v='> 1000'
        else:error=fmt(None if bound is None else bound*1000);v=fmt(None if value is None else value*1000)
        lines.append(f'| {step["target_rps"]} | {fmt(step["p99_ms"])} | {v} | {bucket} | {error} |')
    lines+=['','## Кількість відповідей на кожен екземпляр','',
            'Підрахунок виконано за X-Pod у всіх отриманих відповідях вимірюваних сходинок; прогрів виключено.',
            '', '| N | Екземпляр | Відповідей |','|---|---|---|']
    for n,run in runs.items():
        from collections import Counter
        counts=Counter(r['pod'] for r in run['rows'] if r['pod'] and 10<=r['done']<run['profile_end'])
        for pod,count in sorted(counts.items()):lines.append(f'| {n} | `{pod}` | {count} |')
    lines+=['','## Підбір USL','', 'Формула: X(N)=λN/[1+σ(N−1)+κN(N−1)], λ=X(1).',
            'Для N>1: C=X(N)/X(1), u=N−1, v=N(N−1), y=N/C−1.']
    if fit['valid']:
        lines+=['','| N | C(N) | u | v | y |','|---|---|---|---|---|']
        for t in fit['points']:lines.append(f'| {t["n"]} | {fmt(t["c"],6)} | {t["u"]} | {t["v"]} | {fmt(t["y"],6)} |')
        for k,v in fit['sums'].items():lines.append(f'\n{k} = {fmt(v,8)}.')
        sums=fit['sums']
        lines += ['', f'σ₀=({fmt(sums["v2"],8)}×{fmt(sums["uy"],8)}−{fmt(sums["uv"],8)}×{fmt(sums["vy"],8)})/{fmt(sums["denominator"],8)}={fmt(fit["raw_sigma"],8)}.',
                  f'κ₀=({fmt(sums["u2"],8)}×{fmt(sums["vy"],8)}−{fmt(sums["uv"],8)}×{fmt(sums["uy"],8)})/{fmt(sums["denominator"],8)}={fmt(fit["raw_kappa"],8)}.',
                  f'Після перевірки невід’ємності: σ={fmt(fit["sigma"],8)}, κ={fmt(fit["kappa"],8)}, λ={fmt(fit["lambda_"])} rps.']
        if fit['raw_sigma']<0:lines.append(f'На межі σ=0 використано κ=Σvy/Σv²={fmt(sums["vy"],8)}/{fmt(sums["v2"],8)}.')
        if fit['raw_kappa']<0:lines.append(f'На межі κ=0 використано σ=Σuy/Σu²={fmt(sums["uy"],8)}/{fmt(sums["u2"],8)}.')
        if fit['n_star'] is not None:lines.append(f'N*=√((1−{fmt(fit["sigma"],8)})/{fmt(fit["kappa"],8)})={fmt(fit["n_star"],4)}.')
        elif fit['kappa']==0:lines.append('κ=0: скінченного внутрішнього максимуму немає. Межа λ/σ='+fmt(fit['asymptote'])+' rps.' if fit['sigma']>0 else 'σ=κ=0: модель дає лінійне зростання без скінченної межі.')
        else:lines.append('σ≥1: формула внутрішнього N* не застосовується; максимум моделі на N≥1 лежить при N=1.')
    else:lines+=['',fit['reason']]
    machine=read(OUT/'machine.json')
    lines+=['','## Ресурси','',f'Docker: {machine["docker_cpus"]} CPU, {fmt(machine["docker_memory_bytes"]/2**30)} ГіБ пам’яті.']
    best4=capacities[-1]['best'];run=runs[4]
    samples=[json.loads(s) for s in (folder/'run-4.resources.jsonl').read_text().splitlines() if s.strip()]
    lo,hi=run['start_epoch']+best4['window_start'],run['start_epoch']+best4['window_end']
    in_window=[s for s in samples if lo<=s['timestamp']<hi]
    if in_window:
        snapshot=min(in_window,key=lambda s:abs(s['timestamp']-(lo+hi)/2))
        lines+=['','Знімок docker stats з 15-секундного вікна X(4):','', '| Контейнер | CPU | Пам’ять |','|---|---|---|']
        for row in snapshot.get('containers',[]):lines.append(f'| {row.get("Name")} | {row.get("CPUPerc")} | {row.get("MemUsage")} |')
        gen=snapshot.get('generator')
        if gen:lines.append(f'\nГенератор разом із дочірніми процесами: CPU {fmt(gen["cpu_percent"])}% (100% = одне ядро), сумарний RSS {fmt(gen["rss_bytes"]/2**20)} МіБ.')
        lines.append(f'\nВикористання CPU всього хоста: {fmt(snapshot["host_cpu_percent"])}%.')
    else:lines.append('Знімка ресурсів у вибраному вікні немає; потрібен повторний збір.')
    lines+=['','## Графіки','']
    for image in ('profile-1','profile-4','instances-1','instances-4','throttling','usl'):
        if (FIG/(image+'.png')).exists():lines.append(f'![{image}](figures/{image}.png)\n')
    (ROOT/'RESULTS.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    print('RESULTS.md, CSV tables and figures written.')

if __name__=='__main__':main()
