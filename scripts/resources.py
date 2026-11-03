import psutil

class ProcessTreeSampler:
    def __init__(self,pid):
        self.root=psutil.Process(pid)
        self.cache={}

    def sample(self):
        try:processes=[self.root]+self.root.children(recursive=True)
        except psutil.Error:return None
        members=[]
        for process in processes:
            try:
                if process.pid not in self.cache:
                    self.cache[process.pid]=process
                    process.cpu_percent(None)
                    cpu=0.0
                else:
                    process=self.cache[process.pid]
                    cpu=process.cpu_percent(None)
                members.append(dict(pid=process.pid,cpu_percent=cpu,rss_bytes=process.memory_info().rss))
            except psutil.Error:
                continue
        return dict(cpu_percent=sum(p['cpu_percent'] for p in members),rss_bytes=sum(p['rss_bytes'] for p in members),
                    pid=self.root.pid,members=members,includes_descendants=True)
