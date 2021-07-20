import requests
import time
import json


class Redash(object):
    def __init__(self, redash_url, api_key, pause_time=3):
        self.redash_url = redash_url
        self.session = requests.Session()
        self.session.headers.update({"Authorization": "Key {}".format(api_key)})
        self.pause = pause_time

    def test_credentials(self):
        try:
            self._get("api/session")
            return True
        except requests.exceptions.HTTPError:
            return False

    def queries(self, page=1, page_size=25):
        """GET api/queries"""
        return self._get(
            "api/queries", params=dict(page=page, page_size=page_size)
        ).json()

    def dashboards(self, page=1, page_size=25):
        """GET api/dashboards"""
        return self._get(
            "api/dashboards", params=dict(page=page, page_size=page_size)
        ).json()

    def dashboard(self, slug):
        """GET api/dashboards/{slug}"""
        return self._get("api/dashboards/{}".format(slug)).json()

    def create_dashboard(self, name):
        return self._post("api/dashboards", json={"name": name}).json()

    def update_dashboard(self, dashboard_id, properties):
        return self._post(
            "api/dashboards/{}".format(dashboard_id), json=properties
        ).json()

    def create_widget(self, dashboard_id, visualization_id, text, options):
        data = {
            "dashboard_id": dashboard_id,
            "visualization_id": visualization_id,
            "text": text,
            "options": options,
            "width": 1,
        }
        return self._post("api/widgets", json=data)

    def duplicate_dashboard(self, slug, new_name=None):
        current_dashboard = self.dashboard(slug)

        if new_name is None:
            new_name = "Copy of: {}".format(current_dashboard["name"])

        new_dashboard = self.create_dashboard(new_name)
        if current_dashboard["tags"]:
            self.update_dashboard(
                new_dashboard["id"], {"tags": current_dashboard["tags"]}
            )

        for widget in current_dashboard["widgets"]:
            visualization_id = None
            if "visualization" in widget:
                visualization_id = widget["visualization"]["id"]
            self.create_widget(
                new_dashboard["id"], visualization_id, widget["text"], widget["options"]
            )

        return new_dashboard

    def get_fresh_query_result(self, query_id, params):
        """
        params = {'p_param_name': 99}
        query_id = 1234
        Returns the array of result rows (rows are dicts)
        """

        payload = dict(max_age=0, parameters=params)

        response = self.session.post('{}/api/queries/{}/results'.format(self.redash_url, query_id), data=json.dumps(payload))

        if response.status_code != 200:
            raise Exception('Refresh failed for query {}. {}'.format(query_id, response.text))

        result_id = self.poll_job(response.json()['job'], query_id)

        if result_id:
            response = self.session.get('{}/api/queries/{}/results/{}.json'.format(self.redash_url, query_id, result_id))
            if response.status_code != 200:
                raise Exception('Failed getting results for query {}. {}'.format(query_id, response.text))
        else:
            raise Exception('Failed getting result {}. {}'.format(query_id, response.text))

        return response.json()['query_result']['data']['rows']

    def poll_job(self, job, query_id):
        while job['status'] not in (3, 4):
            response = self.session.get('{}/api/jobs/{}'.format(self.redash_url, job['id']))
            job = response.json().get('job', {'status': 'Error NO JOB IN RESPONSE: {}'.format(json.dumps(response.json()))})
            time.sleep(self.pause)

        if job['status'] == 3:  # 3 = completed
            return job['query_result_id']
        elif job['status'] == 4:  # 3 = ERROR
            raise Exception('Redash Query {} failed: {}'.format(query_id, job['error']))

    def duplicate_query(self, query_id, new_name=None):

        response = self._post(f"api/queries/{query_id}/fork")
        new_query = response.json()

        if not new_name:
            return new_query

        new_query["name"] = new_name

        return self.update_query(new_query.get("id"), new_query).json()

    def scheduled_queries(self):
        """Loads all queries and returns only the scheduled ones."""
        queries = self.paginate(self.queries)
        return filter(lambda query: query["schedule"] is not None, queries)

    def update_query(self, query_id, data):
        """POST /api/queries/{query_id} with the provided data object."""
        path = "api/queries/{}".format(query_id)
        return self._post(path, json=data)

    def paginate(self, resource):
        """Load all items of a paginated resource"""
        stop_loading = False
        page = 1
        page_size = 100

        items = []

        while not stop_loading:
            response = resource(page=page, page_size=page_size)

            items += response["results"]
            page += 1

            stop_loading = response["page"] * response["page_size"] >= response["count"]

        return items

    def _get(self, path, **kwargs):
        return self._request("GET", path, **kwargs)

    def _post(self, path, **kwargs):
        return self._request("POST", path, **kwargs)

    def _request(self, method, path, **kwargs):
        url = "{}/{}".format(self.redash_url, path)
        response = self.session.request(method, url, **kwargs)
        response.raise_for_status()
        return response
