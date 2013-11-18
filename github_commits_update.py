import base64
import redis
import json
import datetime
import dateutil.parser
import httplib2
from config import Config
from instrumentation import *

def parse_timestamp_utc(time_str):
    parsed = dateutil.parser.parse(time_str)
    offset_str = str(parsed).rsplit("+", 1)
    pr = "+"
    if len(offset_str) == 1:
        offset_str = offset_str[0].rsplit("-", 1)
        pr = "-"
    if len(offset_str) == 2:
        offset_str = pr + offset_str[1]
    else:
        offset_str = ""
    if hasattr(parsed.tzinfo, "_offset"):
        offset = parsed.tzinfo._offset
    else:
        offset = datetime.timedelta(0)
    parsed = parsed.replace(tzinfo=None) - offset
    return (parsed, offset_str)

class GithubUpdate:
    def __init__(self, username, password, organization):
        self._db = None
        self.username = username
        self.password = password
        self.organization = organization
        self.auth = base64.encodestring( username + ':' + password)
        self.headers = {"Authorization": "Basic "+ self.auth}
        self.http = httplib2.Http(disable_ssl_certificate_validation=True)
        self.config = Config()
        self.redis = redis.Redis(host=self.config.get("redis-hostname"), port=self.config.get("redis-port"), db=self.config.get("redis-db"))
        self.post_queue = []

    def parse_link(self, link):
        if not link:
            return None
        link = link.split(", ")
        for item in link:
            item = item.split("; ")
            rel = item[1]
            if rel == 'rel="next"':
                return item[0].replace("<", "").replace(">", "")
        return None

    @timing("github.update.get_repositories")
    def get_repositories(self):
        statsd.incr("github.update.get_repositories")
        repos = []
        url = "https://api.github.com/orgs/%s/repos?per_page=100" % self.organization
        (response, content) = self.http.request(url, "GET", headers=self.headers)
        link = self.parse_link(response.get("link"))
        repos = json.loads(content)
        while link:
            (response, content) = self.http.request(link, "GET", headers=self.headers)
            link = self.parse_link(response.get("link"))
            data = json.loads(content)
            if isinstance(data, list) and len(data) > 0:
                repos.extend(data)
            else:
                break
        return repos

    @timing("github.update.get_commits")
    def get_commits(self, name, url, since = None):
        statsd.incr("github.update.get_commits")
        url = url.replace("{/sha}", "")
        if since:
            url = url + "&since=%s" % since
        (response, content) = self.http.request(url, "GET", headers=self.headers)
        link = self.parse_link(response.get("link"))
        commits = json.loads(content)
        return (commits, link)

    @timing("github.update.main")
    def process(self):

        last_processed_save = None


        statsd.incr("github.update.main")
        repositories = self.get_repositories()
        for repo in repositories:
            last_change = repo.get('pushed_at')
            name = repo.get("name")
            commit_url = repo.get("commits_url") + "?per_page=100"
            if not name or not commit_url:
                continue
            repo_key = "github-%s-" % name
            last_processed = self.redis.get(repo_key+"pushed_at")
            if not last_processed:
                last_processed = "1970-01-01T00:00:00"
            if last_processed >= last_change:
                continue
            last_processed_save = last_processed

            link = None
            while True:
                if link is None:
                    (commits, link) = self.get_commits(name, commit_url, last_processed)
                else:
                    (commits, link) = self.get_commits(name, link)
                for commit in commits:
                    if not "commit" in commit:
                        continue
                    d = commit["commit"]
                    if not "author" in d:
                        continue
                    d = d["author"]
                    if not "email" in d or not "date" in d:
                        continue

                    if d["date"] > last_processed_save:
                        last_processed_save = d["date"]

                    if not d["email"].endswith(self.config.get("email-domain")):
                        continue
     
                    if d["date"] > last_processed:
                        parsed, offset_str = parse_timestamp_utc(d["date"])
                        self.post({"system": "github-commits", "timestamp": str(parsed), "is_utc": True, "tzinfo": offset_str, "username": d["email"], "data": name})
                if link is None:
                    break

            if last_processed_save == last_processed:
                last_processed_save = last_change

            self.redis.set(repo_key+"pushed_at", last_processed_save)
            self.post_finished()

    def post_finished(self):
        self.post()

    @timing("github.update.post")
    def post(self, data = None):
        if data is not None:
            self.post_queue.append(data)
        if len(self.post_queue) > 100 or (data is None and len(self.post_queue) > 0):
            statsd.incr("github.update.post.request")
            (_, cont) = self.http.request(self.config.get("server-url"), "POST", body=json.dumps(self.post_queue))
            if cont == "OK":
                self.post_queue = []
            else:
                return False

def main():
    config = Config()
    github = GithubUpdate(config.get("github-username"), config.get("github-password"), config.get("github-organization"))
    github.process()

if __name__ == '__main__':
    main()
