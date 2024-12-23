import datetime
from dynatrace_extension import Extension, Status, StatusValue
import requests
import json
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

class ExtensionImpl(Extension):
     
    def query_tenant_overview_1_hour(self):
        """
        The query method is automatically scheduled to run every 1 hour.
        """
        self.logger.info("Scheduler started for tenant overview page.")

        for endpoint in self.activation_config["endpoints"]:
            baseurl = endpoint["baseURL"]
            tenant_id = endpoint["tenantId"]
            api_key = endpoint["apiKey"]
            # system_id = endpoint["systemId"]
            system_id = None
            self.logger.info(f"Running endpoint with base url '{baseurl}'")
            self.logger.info(f"Running endpoint with tenant id '{tenant_id}'")
            self.logger.info(f"Running endpoint with api key '{api_key}'")
            self.logger.info(f"Running endpoint with system id '{system_id}'")

            token_headers = {
               'x-api-key': api_key
            }

            self.logger.info(f"Trying to generate API token.")
            token_req_body = {}
            token_api_call = requests.post(f"{baseurl}/restapi/v1/tenants/{tenant_id}/token", headers=token_headers, data=token_req_body)
            self.logger.info(f"token api output '{token_api_call}'")
            token_api_response = {}
            if token_api_call.status_code == 201:
                try:
                    token_api_response = token_api_call.json()
                except ValueError:
                    print("Token API response is not a valid json", token_api_call.text)
                    return
            token_value = token_api_response["result"]["token"]
            self.logger.info(f"API token creation was successful.")

            api_headers = {
               'x-api-token': token_value
            }
            storage_system_uuid = {}
            with ThreadPoolExecutor(max_workers=50) as executor:
                future_storage_systems_api = executor.submit(fetch_tenants_api_data, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems?storage-type=block", api_headers)
                if future_storage_systems_api.result() is not None:
                    ###For Block system count
                    alerts_result = future_storage_systems_api.result()
                    storage_sytem = alerts_result.json()
                    self.report_metric("si.tenant.block_count", storage_sytem["total_records"])
                    self.logger.info(f"Total block storage system count is '{storage_sytem['total_records']}'.")
                    for system in storage_sytem["data"]:
                        storage_system_uuid.__setitem__(system["storage_system_id"], system["name"])
                    ###For block Table
                    storage_systems_map={}
                    for item in storage_sytem["data"]:
                        storage_systems_map[item['storage_system_id']] = {"condition": item.get('condition', None), "type": item.get('type', None), "model": item.get('model', None), "serial_number": item.get('serial_number', None), "name" : item.get('name', None), "used_capacity": item.get('used_capacity_bytes', None), "available_capacity": item.get('available_capacity_bytes', None), "total_capacity": item.get('capacity_bytes', None), "mapped_capacity": item.get('mapped_capacity_bytes', None), "unmapped_capacity": item.get('unmapped_capacity_bytes', None), "data_reduction_savings": item.get('data_reduction_savings_bytes', None), "provisioned_capacity": item.get('provisioned_capacity_bytes', None), "capacity_savings": item.get('capacity_savings_bytes', None), "volumes_count": item.get('volumes_count', 0), "pool_count": item.get('pools_count', 0), "fc_port_count": item.get('fc_ports_count', 0), "drives_count": item.get('disks_count', 0), "Mdisks_count": item.get('managed_disks_count', 0), "ip_ports_count": item.get('ip_ports_count', 0)}
                    block_systems_table_dimension = {'condition', 'type', 'model', 'serial_number', 'name'}
                
                    for outer_key, inner_dict in storage_systems_map.items():
                        dimensions={}
                        dimensions['system_id'] = outer_key
                        for key, value in inner_dict.items():
                            if key in block_systems_table_dimension:
                                dimensions[key] = value
                            else:
                                if value is None:
                                   value = -1
                                metricKey = f"si.tenant.block.table.{key}"
                                self.report_metric(metricKey, value, dimensions)
                    self.logger.info(f"Tenant block storage system table metrics pushed to dynatrace.")
                    ###For block health chart
                    counter = Counter(item['condition'] for item in storage_sytem["data"])
                    metrics = [{'name': key, 'value': value} for key, value in counter.items()]
                    for metric in metrics:
                        dimensions={'status': metric['name']}
                        value=metric['value']
                        self.report_metric("si.tenant.block_health_status", value, dimensions)
                    self.logger.info("Tenant block storage system health status metrics pushed to dynatrace.")
                    ###For block capacity
                    capacity={}
                    capacity_name = ["used_capacity_bytes", "available_capacity_bytes", "capacity_bytes"]  
                    for system_data in storage_sytem["data"]:
                        for item in capacity_name:
                            if item in system_data:
                                if item in capacity:
                                    capacity[item] += system_data[item]
                                else:
                                    capacity[item] = system_data[item]

                    capacity_map = [
                        {
                            'name': 
                            'Used Capacity' if key == 'used_capacity_bytes' else 
                            'Available Capacity' if key == 'available_capacity_bytes' else
                            'Total Capacity' if key == 'capacity_bytes' else 
                            key,
                            'value': value
                        }
                        for key, value in capacity.items()
                     ]
                    for item in capacity_map:
                        dimensions={'capacity_name_updated': item['name']}
                        value=item['value']
                        self.report_metric("si.tenant.block_capacity", value, dimensions)
                    self.logger.info(f"Tenant block total, available, used capacity metrics pushed to dynatrace.")
                    ###For host capacity mapping
                    host_capacity={}
                    host_capacity_name = ["mapped_capacity_bytes", "unmapped_capacity_bytes"]  
                    for system_data in storage_sytem["data"]:
                        for item in host_capacity_name:
                            if item in system_data:
                                if item in host_capacity:
                                    host_capacity[item] += system_data[item]
                                else:
                                    host_capacity[item] = system_data[item]

                    host_capacity_map = [
                        {
                            'name': 
                            'Mapped Capacity' if key == 'mapped_capacity_bytes' else 
                            'Unmapped Capacity' if key == 'unmapped_capacity_bytes' else
                            key,
                            'value': value
                        }
                        for key, value in host_capacity.items()
                     ]
                    for item in host_capacity_map:
                        dimensions={'host_capacity_name': item['name']}
                        value=item['value']
                        self.report_metric("si.tenant.host_capacity", value, dimensions)
                    self.logger.info(f"Tenant block host mapped, unmapped capacity metrics pushed to dynatrace.")
                     ###For data reduction, data provisioned and total savings value
                    capacity_saving={}
                    capacity_savings_name = ["data_reduction_savings_bytes", "provisioned_capacity_bytes", "capacity_savings_bytes"]  
                    for system_data in storage_sytem["data"]:
                        for item in capacity_savings_name:
                            if item in system_data:
                                if item in capacity_saving:
                                    capacity_saving[item] += system_data.get(item, 0)
                                else:
                                    capacity_saving[item] = system_data.get(item, 0)

                    capacity_saving_map = [
                        {
                            'name': 
                            'Data Reduction' if key == 'data_reduction_savings_bytes' else 
                            'Data Provisioned' if key == 'provisioned_capacity_bytes' else
                            'Total Savings' if key == 'capacity_savings_bytes' else 
                            key,
                            'value': value
                        }
                        for key, value in capacity_saving.items()
                     ]
                    for item in capacity_saving_map:
                        dimensions={'capacity_savings_type': item['name']}
                        value=item['value']
                        self.report_metric("si.tenant.block_capacity_savings", value, dimensions)
                    self.logger.info(f"Tenant block data reduction, provisioned and capacity savings metrics pushed to dynatrace.")
                alerts_resp = {}
                future_alerts = executor.submit(fetch_tenants_api_data, f"{baseurl}/restapi/v1/tenants/{tenant_id}/alerts?duration=1d", api_headers)
                if future_alerts.result() is not None:
                    alerts_result = future_alerts.result()
                    if alerts_result.status_code == 200:
                        try:
                            alerts_resp = alerts_result.json()
                        except ValueError:
                            self.logger.error(f"Alerts API response is not a valid json '{alerts_result.text}'")
                        self.report_metric("si.tenant.block.alert.type.critical", 0, dimensions)
                        self.report_metric("si.tenant.block.alert.type.warning", 0, dimensions)
                        self.report_metric("si.tenant.block.alert.type.info", 0, dimensions)
                        self.report_metric("si.tenant.block.alert.type.critical_acknowledged", 0, dimensions)
                        self.report_metric("si.tenant.block.alert.type.warning_acknowledged", 0, dimensions)
                        self.report_metric("si.tenant.block.alert.type.info_acknowledged", 0, dimensions)
                        counter = Counter(item['severity'] for item in alerts_resp["data"])
                        alert_severity_types = [{'name': key, 'value': value} for key, value in counter.items()]
                        for alert_type in alert_severity_types:
                            dimensions={'alert_type': alert_type['name']}
                            metricKey = "si.tenant.block.alert.type."+alert_type['name']
                            value=alert_type['value']
                            self.logger.info(f"'{alert_type['name']}' '{value}'")
                            self.report_metric(metricKey, value, dimensions)
                        self.logger.info(f"Tenant alert types metrics pushed to dynatrace.")
                        ##For Alerts Table
                        """ storage_systems_alerts_map={}
                        for item in alerts_resp["data"]:
                            counter = Counter((item['severity'], item['parentResource']) for item in alerts_resp["data"])
                        for (category, name), count_value in counter.items():
                            if name in storage_systems_alerts_map:
                                severity_map = storage_systems_alerts_map[name]
                                severity_map[category] = count_value
                                storage_systems_alerts_map[name] = severity_map
                            else:
                                storage_systems_alerts_map[name] = {category : count_value}
                        for key, value in storage_systems_alerts_map.items():
                           if 'critical' not in value:
                               value['critical'] = 0
                           if 'warning' not in value:
                               value['warning'] = 0
                           if 'info' not in value:
                               value['info'] = 0
                           if 'info_acknowledged' not in value:
                               value['info_acknowledged'] = 0
                           if 'critical_acknowledged' not in value:
                               value['critical_acknowledged'] = 0
                           if 'warning_acknowledged' not in value:
                               value['warning_acknowledged'] = 0
                        self.logger.info(f"storage_systems_alerts_map {storage_systems_alerts_map}")
                        for outer_key, outer_value in storage_systems_alerts_map.items():
                            dimensions={'system_name': outer_key}
                            for key, value in outer_value.items():
                                metricKey = f"si.tenant.block.alert.table.{key}"
                                self.report_metric(metricKey, value, dimensions)
                        self.logger.info(f"Tenant alert table metrics pushed to dynatrace.") """
            
                alerts_map={}
                for item in alerts_resp["data"]:
                    alerts_map[item['Alert ID']] = {"resources": item.get('resource', None), "alert_name": item.get('name', None), "condition": item.get('condition', None), "violation": item.get('violation', None), "severity": item.get('severity', None), "resource_type": item.get('resourceType', None), "category": item.get('category', None), "occurence_time_utc": item.get('occurenceTime', 0), "system": item.get('parentResource', None), "alert_source": item.get('source', None), "occurences": item.get('occurences', 0)}
                alerts_table_dimension = {'resources', 'alert_name', 'condition',  'violation', 'severity', 'resource_type', 'category', 'occurence_time_utc', 'system', 'alert_source'}
            
                for outer_key, inner_dict in alerts_map.items():
                    dimensions={}
                    dimensions['alert_id'] = outer_key
                    for key, value in inner_dict.items():
                        if ('severity', 'critical') in inner_dict.items():
                            if key in alerts_table_dimension:
                                dimensions[key] = value
                                if key == "occurence_time_utc":
                                    occur_date = datetime.datetime.fromtimestamp(value/1000.0, tz=datetime.timezone.utc)
                                    occur_date_format = occur_date.strftime("%d %b %Y, %H:%M:%S")
                                    dimensions[key] = occur_date_format
                            else:
                                if value is None:
                                    value = -1
                                metricKey = f"si.tenant.block.alert.table.critical.{key}"
                                self.report_metric(metricKey, value, dimensions)
                self.logger.info(f"Tenant alert table critical metrics pushed to dynatrace.")
                
                ransomware_alerts_resp ={}
                ransomware_alerts_count = 0
                future_ransomware_alerts = executor.submit(fetch_tenants_api_data, f"{baseurl}/restapi/v1/tenants/{tenant_id}/alerts?type=security", api_headers)    
                if future_ransomware_alerts.result() is not None:
                    ransomware_alerts_result = future_ransomware_alerts.result()
                    if ransomware_alerts_result.status_code == 200:
                        try:
                            ransomware_alerts_resp = ransomware_alerts_result.json()
                        except ValueError:
                            self.logger.error(f"Ransomware alerts API is not a valid json '{ransomware_alerts_result.text}'.")
                    dimensions = {
                        "message": ransomware_alerts_resp["message"]
                    }
                    try:
                        if "total_records" in ransomware_alerts_resp:
                            ransomware_alerts_count = ransomware_alerts_resp["total_records"]
                    except KeyError:
                        self.logger.error(f"total_records property doesn't exist in response.")
                self.logger.info(f"Tenant ransomware alerts count is '{ransomware_alerts_count}'.")
                self.report_metric("si.tenant.ransomware_alerts_count", ransomware_alerts_count, dimensions)
                self.logger.info(f"Tenant ransomware alerts count metrics pushed to dynatrace.")
                """ self.logger.info(f"Trying to invoke tenant performance metrics i/o rate, data rate and response time for all storage systems.")
                current_time_ms = round(datetime.datetime.now().timestamp()*1000)
                self.logger.info(f"Current system time in milliseconds '{current_time_ms}'.")
                allowed_time_past_ms = current_time_ms - 3600000
                allowed_time_future_ms = current_time_ms + 600000
                end_time_ms = current_time_ms
                start_time_ms = end_time_ms - 900000
                self.logger.info(f"Start timestamp in ms '{start_time_ms}', End timestamp in ms '{end_time_ms}'.")
                metrics_resp = {}        
                future_metrics = executor.submit(fetch_tenants_api_data, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/metrics?start-time={start_time_ms}&end-time={end_time_ms}&types=volume_overall_read_io_rate&types=volume_overall_write_io_rate&types=volume_overall_total_io_rate", api_headers)
                if future_metrics.result() is not None:
                    metrics_result = future_metrics.result()
                    if metrics_result.status_code == 200:
                        try:
                            metrics_resp = metrics_result.json()
                        except ValueError:
                            self.logger.error(f"Metrics API i/o rate response is not a valid json '{metrics.text}'.")
                        self.logger.info(f"I/O rate response '{metrics_resp}'")
                        try:
                            if "data" in metrics_resp:
                                for metric in metrics_resp["data"]:
                                    dimensions={'system_name': metric["name"]}
                                    for iorate_metric in metric["metrics"]:
                                        if "time_stamp" in iorate_metric:
                                            metric_timestamp = iorate_metric["time_stamp"]
                                            if metric_timestamp >= allowed_time_past_ms and metric_timestamp <= allowed_time_future_ms:
                                                ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                                                if "volume_overall_read_io_rate" in iorate_metric:
                                                    self.report_metric("si.tenant.block.perf.readiorate", iorate_metric["volume_overall_read_io_rate"], dimensions, timestamp=ts)      
                                                if "volume_overall_write_io_rate" in iorate_metric:
                                                    self.report_metric("si.tenant.block.perf.writeiorate", iorate_metric["volume_overall_write_io_rate"], dimensions, timestamp=ts)
                                                if "volume_overall_total_io_rate" in iorate_metric:
                                                    self.report_metric("si.tenant.block.perf.totaliorate", iorate_metric["volume_overall_total_io_rate"], dimensions, timestamp=ts)
                                            else:
                                                self.logger.error(f"I/O rate metric timestamp '{metric_timestamp}'is not in allowed range, 1 hour from past and 10 mins into future.")
                        except KeyError:
                            self.logger.error(f"data property doesn't exist in i/o rate response.")
                metrics_resp = {}
                future_metrics = executor.submit(fetch_tenants_api_data, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/metrics?start-time={start_time_ms}&end-time={end_time_ms}&types=volume_read_data_rate&types=volume_write_data_rate&types=volume_total_data_rate", api_headers)
                if future_metrics.result() is not None:
                    metrics_result = future_metrics.result()
                    if metrics_result.status_code == 200:
                        try:
                            metrics_resp = metrics_result.json()
                        except ValueError:
                            self.logger.error(f"Metrics API data rate response is not a valid json '{metrics.text}'")
                        self.logger.info(f"data rate response '{metrics_resp}'")
                        try:
                            if "data" in metrics_resp:
                                for metric in metrics_resp["data"]:
                                    dimensions={'system_name': metric["name"]}
                                    for datarate_metric in metric["metrics"]:
                                        if "time_stamp" in datarate_metric:
                                            metric_timestamp = datarate_metric["time_stamp"]
                                            if metric_timestamp >= allowed_time_past_ms and metric_timestamp <= allowed_time_future_ms:
                                                ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                                                if "volume_read_data_rate" in datarate_metric:
                                                    self.report_metric("si.tenant.block.perf.readdatarate", datarate_metric["volume_read_data_rate"], dimensions, timestamp=ts)
                                                if "volume_write_data_rate" in datarate_metric:
                                                    self.report_metric("si.tenant.block.perf.writedatarate", datarate_metric["volume_write_data_rate"], dimensions, timestamp=ts)
                                                if "volume_total_data_rate" in datarate_metric:
                                                    self.report_metric("si.tenant.block.perf.totaldatarate", datarate_metric["volume_total_data_rate"], dimensions, timestamp=ts)
                                            else:
                                                self.logger.error(f"Data rate metric timestamp '{metric_timestamp}'is not in allowed range, 1 hour from past and 10 mins into future.")
                        except KeyError:
                            self.logger.error(f"data property doesn't exist in data rate response.")
                metrics_resp = {}
                future_metrics = executor.submit(fetch_tenants_api_data, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/metrics?start-time={start_time_ms}&end-time={end_time_ms}&types=volume_read_response_time&types=volume_write_response_time&types=volume_total_response_time", api_headers)
                if future_metrics.result() is not None:
                    metrics_result = future_metrics.result()
                    if metrics_result.status_code == 200:
                        try:
                            metrics_resp = metrics_result.json()
                        except ValueError:
                            self.logger.error(f"Metrics API response time is not a valid json '{metrics.text}'.")
                        self.logger.info(f"response time '{metrics_resp}'")
                        try:
                            if "data" in metrics_resp:
                                for metric in metrics_resp["data"]:
                                    dimensions={'system_name': metric["name"]}
                                    for responsetime_metric in metric["metrics"]:
                                        if "time_stamp" in responsetime_metric:
                                            metric_timestamp = responsetime_metric["time_stamp"]
                                            if metric_timestamp >= allowed_time_past_ms and metric_timestamp <= allowed_time_future_ms:
                                                ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                                                if "volume_read_response_time" in responsetime_metric:
                                                    self.report_metric("si.tenant.block.perf.readresponsetime", responsetime_metric["volume_read_response_time"], dimensions, timestamp=ts)
                                                if "volume_write_response_time" in responsetime_metric:
                                                    self.report_metric("si.tenant.block.perf.writeresponsetime", responsetime_metric["volume_write_response_time"], dimensions, timestamp=ts)
                                                if "volume_total_response_time" in responsetime_metric:
                                                    self.report_metric("si.tenant.block.perf.totalresponsetime", responsetime_metric["volume_total_response_time"], dimensions, timestamp=ts) 
                                            else:
                                                self.logger.error(f"Response time metric timestamp '{metric_timestamp}'is not in allowed range, 1 hour from past and 10 mins into future.")           
                        except KeyError:
                            self.logger.error(f"data property doesn't exist in response time.")
                self.logger.info(f"Tenant performance metrics i/o rate, data rate, response time for all storage systems pushed to dynatrace.")
                self.logger.info(f"storage system id '{system_id}'")
                if system_id is not None:
                    self.logger.info(f"Starting to push top 5 volume performance metrics for a given storage system.")
                    metrics_resp = {}
                    future_metrics = executor.submit(fetch_tenants_api_data, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/statistics?duration=5m&type=volume_overall_total_io_rate&compType=luns", api_headers)
                    if future_metrics.result() is not None:
                        metrics_result = future_metrics.result()
                        self.logger.info(f"Top 5 volumes metrics response '{metrics_result}'")
                        if metrics_result.status_code == 200:
                            try:
                                metrics_resp = metrics_result.json()
                            except ValueError:
                                self.logger.error(f"Statistics API volume response is not a valid json '{metrics.text}'")
                        try:
                            if "data" in metrics_resp:
                                for metric in metrics_resp["data"]:
                                    self.logger.info(f"'{metric}'")
                                    dimensions={'volume_name': metric["name"]}
                                    for volume_metric in metric["metrics"]:
                                        if "time_stamp" in volume_metric:
                                            metric_timestamp = volume_metric["time_stamp"]
                                            if metric_timestamp >= allowed_time_past_ms and metric_timestamp <= allowed_time_future_ms:
                                                ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                                                self.report_metric("si.tenant.system.top5vol.perf.totaliorate", volume_metric["volume_overall_total_io_rate"], dimensions, timestamp=ts)
                                            else:
                                                self.logger.error(f"Top 5 volume total io rate metric '{volume_metric['volume_overall_total_io_rate']}' timestamp '{metric_timestamp}'is not in allowed range, 1 hour from past and 10 mins into future.")  

                        except KeyError:
                            self.logger.error(f"data property doesn't exist in top 5 volume total io rate API response.")
                    self.logger.info(f"Top 5 volume total io rate performance metrics pushed for a given storage system.")
                    self.logger.info(f"Starting to push top 5 drive total io rate performance metrics for a given storage system.")
                    metrics_resp = {}
                    future_metrics = executor.submit(fetch_tenants_api_data, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/statistics?duration=5m&type=total_disk_io_rate&compType=disks", api_headers)
                    if future_metrics.result() is not None:
                        metrics_result = future_metrics.result()
                        self.logger.info(f"Top 5 drives metrics response '{metrics_result}'")
                        if metrics_result.status_code == 200:
                            try:
                                metrics_resp = metrics_result.json()
                            except ValueError:
                                self.logger.error(f"Statistics API drive response is not a valid json '{metrics.text}'")
                        try:
                            if "data" in metrics_resp:
                                for metric in metrics_resp["data"]:
                                    self.logger.info(f"'{metric}'")
                                    dimensions={'drive_name': metric["name"]}
                                    for iorate_metric in metric["metrics"]:
                                        if "time_stamp" in iorate_metric:
                                            metric_timestamp = iorate_metric["time_stamp"]
                                            if metric_timestamp >= allowed_time_past_ms and metric_timestamp <= allowed_time_future_ms:
                                                ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                                                self.report_metric("si.tenant.system.top5drive.perf.totaliorate", iorate_metric["total_disk_io_rate"], dimensions, timestamp=ts)
                                            else:
                                                self.logger.error(f"Top 5 drive total i/o rate metric '{iorate_metric['total_disk_io_rate']}' timestamp '{metric_timestamp}'is not in allowed range, 1 hour from past and 10 mins into future.")  
                        except KeyError:
                            self.logger.error("data property doesn't exist in statistics disk total i/o rate API response")
                    self.logger.info(f"Top 5 drive total io rate performance metrics pushed for a given storage system.") """
                # fetch_unified_analysis_screen_metrics(executor, baseurl, tenant_id, api_headers, self)
        
        self.logger.info("Scheduler stopped for tenant overview page.")

    def query_tenant_performance_30_minutes(self):
        """
        The query method is automatically scheduled to run every 30 minutes
        """
        self.logger.info("Scheduler started for tenant performance metrics.")
        end_time= int(datetime.datetime.now().timestamp() * 1000)
        for endpoint in self.activation_config["endpoints"]:
            baseurl = endpoint["baseURL"]
            tenant_id = endpoint["tenantId"]
            api_key = endpoint["apiKey"]
            # system_id = endpoint["systemId"]
            system_id = None
            self.logger.info(f"Running endpoint with base url '{baseurl}'")
            self.logger.info(f"Running endpoint with tenant id '{tenant_id}'")
            self.logger.info(f"Running endpoint with api key '{api_key}'")
            self.logger.info(f"Running endpoint with system id '{system_id}'")

            token_headers = {
               'x-api-key': api_key
            }

            self.logger.info(f"Trying to generate API token.")
            token_req_body = {}
            token_api_call = requests.post(f"{baseurl}/restapi/v1/tenants/{tenant_id}/token", headers=token_headers, data=token_req_body)
            self.logger.info(f"token api output '{token_api_call}'")
            token_api_response = {}
            if token_api_call.status_code == 201:
                try:
                    token_api_response = token_api_call.json()
                except ValueError:
                    print("Token API response is not a valid json", token_api_call.text)
                    return
            token_value = token_api_response["result"]["token"]
            self.logger.info(f"API token creation was successful.")

            api_headers = {
               'x-api-token': token_value
            }
            storage_system_uuid = {}
            with ThreadPoolExecutor(max_workers=50) as executor:
                self.logger.info(f"Trying to invoke tenant performance metrics i/o rate, data rate and response time for all storage systems.")
                current_time_ms = round(datetime.datetime.now().timestamp()*1000)
                self.logger.info(f"Current system time in milliseconds '{current_time_ms}'.")
                allowed_time_past_ms = current_time_ms - 3600000
                allowed_time_future_ms = current_time_ms + 600000
                end_time_ms = current_time_ms
                # Query storage systems performance metrics in last 55 minutes.
                start_time_ms = end_time_ms - 3300000
                self.logger.info(f"Start timestamp in ms '{start_time_ms}', End timestamp in ms '{end_time_ms}'.")
                metrics_resp = {}
                future_metrics = executor.submit(fetch_tenants_api_data, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/metrics?start-time={start_time_ms}&end-time={end_time_ms}&types=volume_overall_read_io_rate&types=volume_overall_write_io_rate&types=volume_overall_total_io_rate", api_headers)
                if future_metrics.result() is not None:
                    metrics_result = future_metrics.result()
                    if metrics_result.status_code == 200:
                        try:
                            metrics_resp = metrics_result.json()
                        except ValueError:
                            self.logger.error(f"Metrics API i/o rate response is not a valid json '{metrics_resp.text}'.")
                        self.logger.info(f"I/O rate response '{metrics_resp}'")
                        try:
                            if "data" in metrics_resp:
                                for metric in metrics_resp["data"]:
                                    dimensions={'system_name': metric["name"]}
                                    for iorate_metric in metric["metrics"]:
                                        if "time_stamp" in iorate_metric:
                                            metric_timestamp = iorate_metric["time_stamp"]
                                            if metric_timestamp >= allowed_time_past_ms and metric_timestamp <= allowed_time_future_ms:
                                                ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                                                if "volume_overall_read_io_rate" in iorate_metric:
                                                    self.report_metric("si.tenant.block.perf.readiorate", iorate_metric["volume_overall_read_io_rate"], dimensions, timestamp=ts)
                                                if "volume_overall_write_io_rate" in iorate_metric:
                                                    self.report_metric("si.tenant.block.perf.writeiorate", iorate_metric["volume_overall_write_io_rate"], dimensions, timestamp=ts)
                                                if "volume_overall_total_io_rate" in iorate_metric:
                                                    self.report_metric("si.tenant.block.perf.totaliorate", iorate_metric["volume_overall_total_io_rate"], dimensions, timestamp=ts)
                                            else:
                                                self.logger.error(f"I/O rate metric timestamp '{metric_timestamp}'is not in allowed range, 1 hour from past and 10 mins into future.")
                        except KeyError:
                            self.logger.error(f"data property doesn't exist in i/o rate response.")
                metrics_resp = {}
                future_metrics = executor.submit(fetch_tenants_api_data, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/metrics?start-time={start_time_ms}&end-time={end_time_ms}&types=volume_read_data_rate&types=volume_write_data_rate&types=volume_total_data_rate", api_headers)
                if future_metrics.result() is not None:
                    metrics_result = future_metrics.result()
                    if metrics_result.status_code == 200:
                        try:
                            metrics_resp = metrics_result.json()
                        except ValueError:
                            self.logger.error(f"Metrics API data rate response is not a valid json '{metrics_resp.text}'")
                        self.logger.info(f"data rate response '{metrics_resp}'")
                        try:
                            if "data" in metrics_resp:
                                for metric in metrics_resp["data"]:
                                    dimensions={'system_name': metric["name"]}
                                    for datarate_metric in metric["metrics"]:
                                        if "time_stamp" in datarate_metric:
                                            metric_timestamp = datarate_metric["time_stamp"]
                                            if metric_timestamp >= allowed_time_past_ms and metric_timestamp <= allowed_time_future_ms:
                                                ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                                                if "volume_read_data_rate" in datarate_metric:
                                                    self.report_metric("si.tenant.block.perf.readdatarate", datarate_metric["volume_read_data_rate"], dimensions, timestamp=ts)
                                                if "volume_write_data_rate" in datarate_metric:
                                                    self.report_metric("si.tenant.block.perf.writedatarate", datarate_metric["volume_write_data_rate"], dimensions, timestamp=ts)
                                                if "volume_total_data_rate" in datarate_metric:
                                                    self.report_metric("si.tenant.block.perf.totaldatarate", datarate_metric["volume_total_data_rate"], dimensions, timestamp=ts)
                                            else:
                                                self.logger.error(f"Data rate metric timestamp '{metric_timestamp}'is not in allowed range, 1 hour from past and 10 mins into future.")
                        except KeyError:
                            self.logger.error(f"data property doesn't exist in data rate response.")
                metrics_resp = {}
                future_metrics = executor.submit(fetch_tenants_api_data, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/metrics?start-time={start_time_ms}&end-time={end_time_ms}&types=volume_read_response_time&types=volume_write_response_time&types=volume_total_response_time", api_headers)
                if future_metrics.result() is not None:
                    metrics_result = future_metrics.result()
                    if metrics_result.status_code == 200:
                        try:
                            metrics_resp = metrics_result.json()
                        except ValueError:
                            self.logger.error(f"Metrics API response time is not a valid json '{metrics_resp.text}'.")
                        self.logger.info(f"response time '{metrics_resp}'")
                        try:
                            if "data" in metrics_resp:
                                for metric in metrics_resp["data"]:
                                    dimensions={'system_name': metric["name"]}
                                    for responsetime_metric in metric["metrics"]:
                                        if "time_stamp" in responsetime_metric:
                                            metric_timestamp = responsetime_metric["time_stamp"]
                                            if metric_timestamp >= allowed_time_past_ms and metric_timestamp <= allowed_time_future_ms:
                                                ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                                                if "volume_read_response_time" in responsetime_metric:
                                                    self.report_metric("si.tenant.block.perf.readresponsetime", responsetime_metric["volume_read_response_time"], dimensions, timestamp=ts)
                                                if "volume_write_response_time" in responsetime_metric:
                                                    self.report_metric("si.tenant.block.perf.writeresponsetime", responsetime_metric["volume_write_response_time"], dimensions, timestamp=ts)
                                                if "volume_total_response_time" in responsetime_metric:
                                                    self.report_metric("si.tenant.block.perf.totalresponsetime", responsetime_metric["volume_total_response_time"], dimensions, timestamp=ts)
                                            else:
                                                self.logger.error(f"Response time metric timestamp '{metric_timestamp}'is not in allowed range, 1 hour from past and 10 mins into future.")
                        except KeyError:
                            self.logger.error(f"data property doesn't exist in response time.")
                self.logger.info(f"Tenant performance metrics i/o rate, data rate, response time for all storage systems pushed to dynatrace.")

                self.logger.info(f"Trying to invoke tenant top 5 volume performance metrics total io rate, total response time.")
                future_storage_systems_api = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems?storage-type=block", api_headers)
                if future_storage_systems_api.result() is not None:
                    storage_sytem = future_storage_systems_api.result()
                    vol_overall_tot_io_rate_stat_futures=[]
                    vol_tot_resp_time_stat_futures=[]
                    for item in storage_sytem:
                        system_id = item['storage_system_id']
                        # Query top 5 volume performance metrics in last 30 minutes.
                        start_time= end_time-1800000
                        self.logger.info(f"Storage system id '{system_id}, Start timestamp in ms '{start_time}', End timestamp in ms '{end_time}'.")
                        vol_overall_tot_io_rate_stat_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/statistics?type=volume_overall_total_io_rate&compType=luns&start-time={start_time}&end-time={end_time}&limit=50", api_headers)
                        vol_tot_resp_time_stat_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/statistics?type=volume_total_response_time&compType=luns&start-time={start_time}&end-time={end_time}&limit=50", api_headers)
                        vol_overall_tot_io_rate_stat_futures.append(vol_overall_tot_io_rate_stat_future)
                        vol_tot_resp_time_stat_futures.append(vol_tot_resp_time_stat_future)

                        dimensions={'block_system_name': item['name'], 'system_id':item['storage_system_id']}
                        self.report_metric("si.tenant.block.systems", item.get('capacity_bytes', 0), dimensions)

                for future in as_completed(vol_overall_tot_io_rate_stat_futures):
                    response_data = future.result()
                    for item in response_data or []:
                        self.logger.info(f"volume overall total io rate: '{item}'")
                        dimensions={'vol_name': item.get('name', None), 'system_id': item['system_id']}
                        for metric in item['metrics']:
                            metric_timestamp = metric['time_stamp']
                            ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                            self.report_metric("si.tenant.block.system.volumes.overall.tot.io.rate.utilization", metric['volume_overall_total_io_rate'], dimensions, timestamp=ts)

                for future in as_completed(vol_tot_resp_time_stat_futures):
                    response_data = future.result()
                    for item in response_data or []:
                        self.logger.info(f"volume total response time: '{item}'")
                        dimensions={'vol_name': item.get('name', None), 'system_id': item['system_id']}
                        for metric in item['metrics']:
                            metric_timestamp = metric['time_stamp']
                            ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                            self.report_metric("si.tenant.block.system.volumes.tot.response.time.rate.utilization", metric['volume_total_response_time'], dimensions, timestamp=ts)
                self.logger.info(f"Top 5 volume performance metrics total io rate, total response time pushed to dynatrace.")

        self.logger.info("Scheduler stopped for tenant performance metrics page.")
    
    def query_tenant_internal_resources_2_hours(self):
        self.logger.info("Scheduler started for tenant internal resources page.")
        for endpoint in self.activation_config["endpoints"]:
            baseurl = endpoint["baseURL"]
            tenant_id = endpoint["tenantId"]
            api_key = endpoint["apiKey"]
            # system_id = endpoint["systemId"]
            system_id = None
            self.logger.info(f"Running endpoint with base url '{baseurl}'")
            self.logger.info(f"Running endpoint with tenant id '{tenant_id}'")
            self.logger.info(f"Running endpoint with api key '{api_key}'")
            self.logger.info(f"Running endpoint with system id '{system_id}'")

            token_headers = {
               'x-api-key': api_key
            }
            self.logger.info(f"Trying to generate API token.")
            token_req_body = {}
            token_api_call = requests.post(f"{baseurl}/restapi/v1/tenants/{tenant_id}/token", headers=token_headers, data=token_req_body)
            self.logger.info(f"token api output '{token_api_call}'")
            token_api_response = {}
            if token_api_call.status_code == 201:
                try:
                    token_api_response = token_api_call.json()
                except ValueError:
                    print("Token API response is not a valid json", token_api_call.text)
                    return
            token_value = token_api_response["result"]["token"]
            self.logger.info(f"API token creation was successful.")

            api_headers = {
               'x-api-token': token_value
            }
            with ThreadPoolExecutor(max_workers=50) as executor:
                future_storage_systems_api = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems?storage-type=block", api_headers)
                if future_storage_systems_api.result() is not None:
                    storage_sytem = future_storage_systems_api.result()
                            
                    volume_futures_to_system_id = {}
                    pool_futures_to_system_id = {}
                    fcport_futures_to_system_id = {}
                    drive_futures_to_system_id = {}
                    ipport_futures_to_system_id = {}
                    mdisk_futures_to_system_id = {}

                    vol_read_data_rate_stat_futures=[]
                    vol_write_data_rate_stat_futures=[]
                    vol_total_data_rate_stat_futures=[]
                    vol_overall_tot_io_rate_stat_futures=[]
                    vol_tot_resp_time_stat_futures=[]
                    vol_overall_read_cache_hit_stat_futures=[]
                    disk_tot_io_rate_stat_futures=[]
                    disk_total_data_rate_stat_futures=[]
                    disk_total_resp_time_stat_futures=[]

                    for item in storage_sytem:
                        system_id = item['storage_system_id']
                        storage_system_volumes_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/volumes", api_headers)
                        storage_system_pools_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/pools", api_headers)
                        # storage_system_fcports_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/fc-ports", api_headers)
                        storage_system_drives_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/drives", api_headers)
                        """ storage_system_ipports_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/ip-ports", api_headers)
                        storage_system_mdisks_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/managed-disks", api_headers) """
                            
                        volume_futures_to_system_id[storage_system_volumes_future] = system_id
                        pool_futures_to_system_id[storage_system_pools_future] = system_id
                        # fcport_futures_to_system_id[storage_system_fcports_future] = system_id
                        drive_futures_to_system_id[storage_system_drives_future] = system_id
                        """ ipport_futures_to_system_id[storage_system_ipports_future] = system_id
                        mdisk_futures_to_system_id[storage_system_mdisks_future] = system_id """
                            
                        """ end_time= int(datetime.datetime.now().timestamp() * 1000)
                        start_time= end_time-300000

                        vol_read_data_rate_stat_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/statistics?type=volume_read_data_rate&compType=luns&start-time={start_time}&end-time={end_time}&limit=50", api_headers)
                        vol_write_data_rate_stat_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/statistics?type=volume_write_data_rate&compType=luns&start-time={start_time}&end-time={end_time}&limit=50", api_headers)
                        vol_total_data_rate_stat_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/statistics?type=volume_total_data_rate&compType=luns&start-time={start_time}&end-time={end_time}&limit=50", api_headers)
                        vol_overall_tot_io_rate_stat_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/statistics?type=volume_overall_total_io_rate&compType=luns&start-time={start_time}&end-time={end_time}&limit=50", api_headers)
                        vol_tot_resp_time_stat_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/statistics?type=volume_total_response_time&compType=luns&start-time={start_time}&end-time={end_time}&limit=50", api_headers)
                        vol_overall_read_cache_hit_stat_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/statistics?type=volume_overall_read_cache_hit_percentage&compType=luns&start-time={start_time}&end-time={end_time}&limit=50", api_headers)
                        disk_tot_io_rate_stat_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/statistics?type=total_disk_io_rate&compType=disks&start-time={start_time}&end-time={end_time}&limit=50", api_headers)
                        disk_total_data_rate_stat_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/statistics?type=disk_total_data_rate&compType=disks&start-time={start_time}&end-time={end_time}&limit=50", api_headers)
                        disk_total_resp_time_stat_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/statistics?type=disk_total_response_time&compType=disks&start-time={start_time}&end-time={end_time}&limit=50", api_headers)

                            
                        vol_read_data_rate_stat_futures.append(vol_read_data_rate_stat_future)
                        vol_write_data_rate_stat_futures.append(vol_write_data_rate_stat_future)
                        vol_total_data_rate_stat_futures.append(vol_total_data_rate_stat_future)
                        vol_overall_tot_io_rate_stat_futures.append(vol_overall_tot_io_rate_stat_future)
                        vol_tot_resp_time_stat_futures.append(vol_tot_resp_time_stat_future)
                        vol_overall_read_cache_hit_stat_futures.append(vol_overall_read_cache_hit_stat_future)
                        disk_tot_io_rate_stat_futures.append(disk_tot_io_rate_stat_future)
                        disk_total_data_rate_stat_futures.append(disk_total_data_rate_stat_future)
                        disk_total_resp_time_stat_futures.append(disk_total_resp_time_stat_future) """
                        
                        
                        dimensions={'block_system_name': item['name'], 'system_id':item['storage_system_id']}
                        self.report_metric("si.tenant.block.systems", item.get('capacity_bytes', 0), dimensions)
                        
                    for storage_system_volumes_future in as_completed(volume_futures_to_system_id):
                        try:
                            system_id = volume_futures_to_system_id[storage_system_volumes_future]
                            response_data = storage_system_volumes_future.result()
                            # self.logger.info(f"'{response_data}'")
                            for item in response_data or []:
                                dimensions={'system_id':system_id, 'volume_natural_key':item.get('naturalKey', None), 'volume_id':item.get('volume_id', None), 'volume_name':item.get('name', None), 'volume_status':item.get('status_label', None), 'volume_pool_name':item.get('pool_name', None), 'hosts':item.get('hosts', None), 'compressed':item.get('compressed', None), 'io_group':item.get('io_group', None), 'node':item.get('node', None), 'thin_provisioned':item.get('thin_provisioned', None), 'capacity_bytes':item.get('capacity_bytes', None)}
                                self.report_metric("si.tenant.block.system.volumes", item.get('capacity_bytes', 0), dimensions)
                        except requests.exceptions.RequestException as e:
                            print(f"An error occurred while calling internal component API: {e}")

                    for storage_system_pools_future in as_completed(pool_futures_to_system_id):
                        try:
                            system_id = pool_futures_to_system_id[storage_system_pools_future]
                            response_data = storage_system_pools_future.result()
                            # self.logger.info(f"'{response_data}'")
                            for item in response_data or []:
                                dimensions={'system_id':system_id, 'pool_natural_key':item.get('natural_key', None), 'pool_name':item.get('name', None), 'pool_status':item.get('status', None), 'encryption':item.get('encryption', None), 'mdisks_count':item.get('mdisks_count', None), 'drives_count':item.get('drives_count', None), 'volumes_count':item.get('volumes_count', None) , 'solid_state':item.get('solid_state', None), 'used_capacity_bytes':item.get('used_capacity_bytes', None), 'available_capacity_bytes':item.get('available_capacity_bytes', None), 'compression_ratio':item.get('total_compression_ratio', None)}
                                self.report_metric("si.tenant.block.system.pools", item.get('usable_capacity_bytes', 0), dimensions)
                        except requests.exceptions.RequestException as e:
                            print(f"An error occurred while calling internal component API: {e}")

                        """ for storage_system_fcports_future in as_completed(fcport_futures_to_system_id):
                            try:
                                system_id = fcport_futures_to_system_id[storage_system_fcports_future]
                                response_data = storage_system_fcports_future.result()
                                for item in response_data or []:
                                    dimensions={'system_id':system_id, 'fcport_natural_key':item.get('natural_key', None), 'fcport_name':item.get('name', None), 'fcport_status':item.get('status', None), 'node':item.get('node', None), 'speed_gbps':item.get('speed_gbps', None)}
                                    self.report_metric("si.tenant.block.system.fcports", item.get('speed_gbps',0), dimensions)
                            except requests.exceptions.RequestException as e:
                                print(f"An error occurred while calling internal component API: {e}") """

                    for storage_system_drives_future in as_completed(drive_futures_to_system_id):
                        try:
                            system_id = drive_futures_to_system_id[storage_system_drives_future]
                            response_data = storage_system_drives_future.result()
                            # self.logger.info(f"'{response_data}'")
                            for item in response_data or []:
                                dimensions={'system_id':system_id, 'drive_natural_key':item.get('natural_key', None), 'drive_name':item.get('name', None), 'drive_status':item.get('status', None), 'raid_array':item.get('raid_array', None), 'class':item.get('class', None), 'speed_rpm':item.get('speed_rpm', None), 'capacity_bytes':item.get('capacity_bytes', None), 'encryption':item.get('encryption', None), 'vendor':item.get('vendor', None), 'model':item.get('model', None), 'serial_number':item.get('serial_number', None), 'firmware':item.get('firmware', None), 'compressed':item.get('compressed', None)}
                                self.report_metric("si.tenant.block.system.drives", item.get('capacity_bytes', 0), dimensions)
                        except requests.exceptions.RequestException as e:
                            print(f"An error occurred while calling internal component API: {e}")

                        """ for storage_system_ipports_future in as_completed(ipport_futures_to_system_id):
                            try:
                                system_id = ipport_futures_to_system_id[storage_system_ipports_future]
                                response_data = storage_system_ipports_future.result()
                                for item in response_data or []:
                                    dimensions={'system_id':system_id, 'ipport_natural_key':item.get('natural_key', None), 'ipport_name':item.get('name', None), 'ipport_status':item.get('status', None), 'acknowledged':item.get('acknowledged', None), 'ip_address':item.get('ip_address', None), 'iqn':item.get('iqn', None), 'is_host_attached':item.get('is_host_attached', None), 'is_storage_attached':item.get('is_storage_attached', None), 'management':item.get('management', None), 'node':item.get('node', None), 'speed_gbps':item.get('speed_gbps', None), 'storage_system':item.get('storage_system', None)}
                                    self.report_metric("si.tenant.block.system.ipports", item.get('speed_gbps', 0), dimensions)
                            except requests.exceptions.RequestException as e:
                                print(f"An error occurred while calling internal component API: {e}")

                        for storage_system_mdisks_future in as_completed(mdisk_futures_to_system_id):
                            try:
                                system_id = mdisk_futures_to_system_id[storage_system_mdisks_future]
                                response_data = storage_system_mdisks_future.result()
                                for item in response_data or []:
                                    dimensions={'system_id':system_id, 'mdisk_natural_key':item.get('natural_key', None), 'mdisk_name':item.get('name', None), 'mdisk_status':item.get('status', None), 'available_capacity_bytes':item.get('available_capacity_bytes', None), 'capacity_bytes':item.get('capacity_bytes', None), 'class':item.get('class', None), 'drive_compression_ratio':item.get('drive_compression_ratio', None), 'mode':item.get('mode', None), 'pool':item.get('pool', None), 'total_compression_ratio':item.get('total_compression_ratio', None), 'volumes_count':item.get('volumes_count', None)}
                                    self.report_metric("si.tenant.block.system.mdisks", item.get('capacity_bytes', 0), dimensions)
                            except requests.exceptions.RequestException as e:
                                print(f"An error occurred while calling internal component API: {e}")
                            
                        for future in as_completed(vol_read_data_rate_stat_futures):
                            response_data = future.result()
                            for item in response_data or []:
                                dimensions={'vol_name': item.get('name', None), 'system_id': item['system_id']}
                                for metric in item['metrics']:
                                metric_timestamp = metric['time_stamp']
                                ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                                self.logger.info(f"sending metric vol read rate : {metric['volume_read_data_rate']} at : {ts} for system : {item['system_id']} for volume : {item['name']}")
                                self.report_metric("si.tenant.block.system.volumes.read.data.rate.utilization", metric['volume_read_data_rate'], dimensions, timestamp=ts)
                                
                        for future in as_completed(vol_write_data_rate_stat_futures):
                            response_data = future.result()
                            for item in response_data or []:
                                dimensions={'vol_name': item.get('name', None), 'system_id': item['system_id']}
                                for metric in item['metrics']:
                                metric_timestamp = metric['time_stamp']
                                ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                                self.report_metric("si.tenant.block.system.volumes.write.data.rate.utilization", metric['volume_write_data_rate'], dimensions, timestamp=ts)

                        for future in as_completed(vol_total_data_rate_stat_futures):
                            response_data = future.result()
                            for item in response_data or []:
                                dimensions={'vol_name': item.get('name', None), 'system_id': item['system_id']}
                                for metric in item['metrics']:
                                metric_timestamp = metric['time_stamp']
                                ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                                self.report_metric("si.tenant.block.system.volumes.total.data.rate.utilization", metric['volume_total_data_rate'], dimensions, timestamp=ts)
                            

                        for future in as_completed(vol_overall_tot_io_rate_stat_futures):
                            response_data = future.result()
                            for item in response_data or []:
                                dimensions={'vol_name': item.get('name', None), 'system_id': item['system_id']}
                                for metric in item['metrics']:
                                metric_timestamp = metric['time_stamp']
                                ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                                self.report_metric("si.tenant.block.system.volumes.overall.tot.io.rate.utilization", metric['volume_overall_total_io_rate'], dimensions, timestamp=ts)

                        for future in as_completed(vol_tot_resp_time_stat_futures):
                            response_data = future.result()
                            for item in response_data or []:
                                dimensions={'vol_name': item.get('name', None), 'system_id': item['system_id']}
                                for metric in item['metrics']:
                                metric_timestamp = metric['time_stamp']
                                ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                                self.report_metric("si.tenant.block.system.volumes.tot.response.time.rate.utilization", metric['volume_total_response_time'], dimensions, timestamp=ts)

                        for future in as_completed(vol_overall_read_cache_hit_stat_futures):
                            response_data = future.result()
                            for item in response_data or []:
                                dimensions={'vol_name': item.get('name', None), 'system_id': item['system_id']}
                                for metric in item['metrics']:
                                metric_timestamp = metric['time_stamp']
                                ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                                self.report_metric("si.tenant.block.system.volumes.overall.read.cache.hit.per.utilization", metric['volume_overall_read_cache_hit_percentage'], dimensions, timestamp=ts)

                        for future in as_completed(disk_tot_io_rate_stat_futures):
                            response_data = future.result()
                            for item in response_data or []:
                                dimensions={'disk_name': item.get('name', None), 'system_id': item['system_id']}
                                for metric in item['metrics']:
                                metric_timestamp = metric['time_stamp']
                                ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                                self.report_metric("si.tenant.block.system.drives.total.io.rate.utilization", metric['total_disk_io_rate'], dimensions, timestamp=ts)

                        for future in as_completed(disk_total_data_rate_stat_futures):
                            response_data = future.result()
                            for item in response_data or []:
                                dimensions={'disk_name': item.get('name', None),'system_id': item['system_id']}
                                for metric in item['metrics']:
                                metric_timestamp = metric['time_stamp']
                                ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                                self.report_metric("si.tenant.block.system.drives.total.data.rate.utilization", metric['disk_total_data_rate'], dimensions, timestamp=ts)

                        for future in as_completed(disk_total_resp_time_stat_futures):
                            response_data = future.result()
                            for item in response_data or []:
                                dimensions={'disk_name': item.get('name', None), 'system_id': item['system_id']}
                                for metric in item['metrics']:
                                metric_timestamp = metric['time_stamp']
                                ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                                self.report_metric("si.tenant.block.system.drives.total.resp.time.utilization", metric['disk_total_response_time'], dimensions, timestamp=ts) """
                
        self.logger.info("Scheduler stopped for tenant internal resources page.")

    def fastcheck(self) -> Status:
        """
        This is called when the extension runs for the first time.
        If this AG cannot run this extension, raise an Exception or return StatusValue.ERROR!
        """
        return Status(StatusValue.OK)
    
    def initialize(self):
    # Initialize tenant overview, performance metrics, internal resources schedulers.
        self.logger.info("Initializing the schedulers to trigger, populate data for tenant overview, performance metrics, interal resources dashboards.")
        self.schedule(self.query_tenant_overview_1_hour, datetime.timedelta(hours=1))
        self.schedule(self.query_tenant_performance_30_minutes, datetime.timedelta(minutes=30))
        self.schedule(self.query_tenant_internal_resources_2_hours, datetime.timedelta(hours=2))

def main():
    ExtensionImpl(name="storage_insights_extension").run()

def fetch_tenants_api_data(url, api_headers):
        try:
            response = requests.get(url, headers=api_headers)
            return response
        except requests.exceptions.RequestException as e:
            print("Internal error occured while calling: {url} : {e}")
            return None

def fetch_tenants_api_data_array(url, api_headers):
        try:
            all_data = []
            response = requests.get(url, headers=api_headers)
            if response.status_code == 200:
              response_json = response.json()
              if response_json and response_json.get('data') and len(response_json['data']) > 0:
                all_data = response_json['data']
            return all_data
        except requests.exceptions.RequestException as e:
            print("Internal error occured while calling: {url} : {e}")
            return None

def fetch_unified_analysis_screen_metrics(executor, baseurl, tenant_id, api_headers, self): 
        print("Enter fetch_unified_analysis_screen_metrics method")
        future_storage_systems_api = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems?storage-type=block", api_headers)           
        if future_storage_systems_api.result() is not None:
            storage_sytem = future_storage_systems_api.result()
                     
            volume_futures_to_system_id = {}
            pool_futures_to_system_id = {}
            fcport_futures_to_system_id = {}
            drive_futures_to_system_id = {}
            ipport_futures_to_system_id = {}
            mdisk_futures_to_system_id = {}

            vol_read_data_rate_stat_futures=[]
            vol_write_data_rate_stat_futures=[]
            vol_total_data_rate_stat_futures=[]
            vol_overall_tot_io_rate_stat_futures=[]
            vol_tot_resp_time_stat_futures=[]
            vol_overall_read_cache_hit_stat_futures=[]
            disk_tot_io_rate_stat_futures=[]
            disk_total_data_rate_stat_futures=[]
            disk_total_resp_time_stat_futures=[]

            for item in storage_sytem:
                system_id = item['storage_system_id']
                storage_system_volumes_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/volumes", api_headers)
                storage_system_pools_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/pools", api_headers)
                # storage_system_fcports_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/fc-ports", api_headers)
                storage_system_drives_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/drives", api_headers)
                """ storage_system_ipports_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/ip-ports", api_headers)
                storage_system_mdisks_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/managed-disks", api_headers) """
                    
                volume_futures_to_system_id[storage_system_volumes_future] = system_id
                pool_futures_to_system_id[storage_system_pools_future] = system_id
                # fcport_futures_to_system_id[storage_system_fcports_future] = system_id
                drive_futures_to_system_id[storage_system_drives_future] = system_id
                """ ipport_futures_to_system_id[storage_system_ipports_future] = system_id
                mdisk_futures_to_system_id[storage_system_mdisks_future] = system_id """
                       
                """ end_time= int(datetime.datetime.now().timestamp() * 1000)
                start_time= end_time-300000

                vol_read_data_rate_stat_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/statistics?type=volume_read_data_rate&compType=luns&start-time={start_time}&end-time={end_time}&limit=50", api_headers)
                vol_write_data_rate_stat_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/statistics?type=volume_write_data_rate&compType=luns&start-time={start_time}&end-time={end_time}&limit=50", api_headers)
                vol_total_data_rate_stat_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/statistics?type=volume_total_data_rate&compType=luns&start-time={start_time}&end-time={end_time}&limit=50", api_headers)
                vol_overall_tot_io_rate_stat_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/statistics?type=volume_overall_total_io_rate&compType=luns&start-time={start_time}&end-time={end_time}&limit=50", api_headers)
                vol_tot_resp_time_stat_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/statistics?type=volume_total_response_time&compType=luns&start-time={start_time}&end-time={end_time}&limit=50", api_headers)
                vol_overall_read_cache_hit_stat_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/statistics?type=volume_overall_read_cache_hit_percentage&compType=luns&start-time={start_time}&end-time={end_time}&limit=50", api_headers)
                disk_tot_io_rate_stat_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/statistics?type=total_disk_io_rate&compType=disks&start-time={start_time}&end-time={end_time}&limit=50", api_headers)
                disk_total_data_rate_stat_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/statistics?type=disk_total_data_rate&compType=disks&start-time={start_time}&end-time={end_time}&limit=50", api_headers)
                disk_total_resp_time_stat_future = executor.submit(fetch_tenants_api_data_array, f"{baseurl}/restapi/v1/tenants/{tenant_id}/storage-systems/{system_id}/statistics?type=disk_total_response_time&compType=disks&start-time={start_time}&end-time={end_time}&limit=50", api_headers)

                       
                vol_read_data_rate_stat_futures.append(vol_read_data_rate_stat_future)
                vol_write_data_rate_stat_futures.append(vol_write_data_rate_stat_future)
                vol_total_data_rate_stat_futures.append(vol_total_data_rate_stat_future)
                vol_overall_tot_io_rate_stat_futures.append(vol_overall_tot_io_rate_stat_future)
                vol_tot_resp_time_stat_futures.append(vol_tot_resp_time_stat_future)
                vol_overall_read_cache_hit_stat_futures.append(vol_overall_read_cache_hit_stat_future)
                disk_tot_io_rate_stat_futures.append(disk_tot_io_rate_stat_future)
                disk_total_data_rate_stat_futures.append(disk_total_data_rate_stat_future)
                disk_total_resp_time_stat_futures.append(disk_total_resp_time_stat_future) """
                
                for storage_system_volumes_future in as_completed(volume_futures_to_system_id):
                    try:
                        system_id = volume_futures_to_system_id[storage_system_volumes_future]
                        response_data = storage_system_volumes_future.result() 
                        for item in response_data or []:
                            dimensions={'system_id':system_id, 'volume_natural_key':item.get('naturalKey', None), 'volume_id':item.get('volume_id', None), 'volume_name':item.get('name', None), 'volume_status':item.get('status_label', None), 'volume_pool_name':item.get('pool_name', None), 'hosts':item.get('hosts', None), 'compressed':item.get('compressed', None), 'io_group':item.get('io_group', None), 'node':item.get('node', None), 'thin_provisioned':item.get('thin_provisioned', None), 'capacity_bytes':item.get('capacity_bytes', None)}
                            self.report_metric("si.tenant.block.system.volumes", item.get('capacity_bytes', 0), dimensions)
                    except requests.exceptions.RequestException as e:
                          print(f"An error occurred while calling internal component API: {e}")

                for storage_system_pools_future in as_completed(pool_futures_to_system_id):
                    try:
                        system_id = pool_futures_to_system_id[storage_system_pools_future]
                        response_data = storage_system_pools_future.result() 
                        for item in response_data or []:
                            dimensions={'system_id':system_id, 'pool_natural_key':item.get('natural_key', None), 'pool_name':item.get('name', None), 'pool_status':item.get('status', None), 'encryption':item.get('encryption', None), 'mdisks_count':item.get('mdisks_count', None), 'drives_count':item.get('drives_count', None), 'volumes_count':item.get('volumes_count', None) , 'solid_state':item.get('solid_state', None), 'used_capacity_bytes':item.get('used_capacity_bytes', None), 'available_capacity_bytes':item.get('available_capacity_bytes', None), 'compression_ratio':item.get('total_compression_ratio', None)}
                            self.report_metric("si.tenant.block.system.pools", item.get('usable_capacity_bytes', 0), dimensions)
                    except requests.exceptions.RequestException as e:
                          print(f"An error occurred while calling internal component API: {e}")

                """ for storage_system_fcports_future in as_completed(fcport_futures_to_system_id):
                    try:
                        system_id = fcport_futures_to_system_id[storage_system_fcports_future]
                        response_data = storage_system_fcports_future.result() 
                        for item in response_data or []:
                            dimensions={'system_id':system_id, 'fcport_natural_key':item.get('natural_key', None), 'fcport_name':item.get('name', None), 'fcport_status':item.get('status', None), 'node':item.get('node', None), 'speed_gbps':item.get('speed_gbps', None)}
                            self.report_metric("si.tenant.block.system.fcports", item.get('speed_gbps',0), dimensions)
                    except requests.exceptions.RequestException as e:
                          print(f"An error occurred while calling internal component API: {e}") """

                for storage_system_drives_future in as_completed(drive_futures_to_system_id):
                    try:
                        system_id = drive_futures_to_system_id[storage_system_drives_future]
                        response_data = storage_system_drives_future.result() 
                        for item in response_data or []:
                            dimensions={'system_id':system_id, 'drive_natural_key':item.get('natural_key', None), 'drive_name':item.get('name', None), 'drive_status':item.get('status', None), 'raid_array':item.get('raid_array', None), 'class':item.get('class', None), 'speed_rpm':item.get('speed_rpm', None), 'capacity_bytes':item.get('capacity_bytes', None), 'encryption':item.get('encryption', None), 'vendor':item.get('vendor', None), 'model':item.get('model', None), 'serial_number':item.get('serial_number', None), 'firmware':item.get('firmware', None), 'compressed':item.get('compressed', None)}
                            self.report_metric("si.tenant.block.system.drives", item.get('capacity_bytes', 0), dimensions)
                    except requests.exceptions.RequestException as e:
                          print(f"An error occurred while calling internal component API: {e}")

                """ for storage_system_ipports_future in as_completed(ipport_futures_to_system_id):
                    try:
                        system_id = ipport_futures_to_system_id[storage_system_ipports_future]
                        response_data = storage_system_ipports_future.result() 
                        for item in response_data or []:
                            dimensions={'system_id':system_id, 'ipport_natural_key':item.get('natural_key', None), 'ipport_name':item.get('name', None), 'ipport_status':item.get('status', None), 'acknowledged':item.get('acknowledged', None), 'ip_address':item.get('ip_address', None), 'iqn':item.get('iqn', None), 'is_host_attached':item.get('is_host_attached', None), 'is_storage_attached':item.get('is_storage_attached', None), 'management':item.get('management', None), 'node':item.get('node', None), 'speed_gbps':item.get('speed_gbps', None), 'storage_system':item.get('storage_system', None)}
                            self.report_metric("si.tenant.block.system.ipports", item.get('speed_gbps', 0), dimensions)
                    except requests.exceptions.RequestException as e:
                          print(f"An error occurred while calling internal component API: {e}")

                for storage_system_mdisks_future in as_completed(mdisk_futures_to_system_id):
                    try:
                        system_id = mdisk_futures_to_system_id[storage_system_mdisks_future]
                        response_data = storage_system_mdisks_future.result() 
                        for item in response_data or []:
                            dimensions={'system_id':system_id, 'mdisk_natural_key':item.get('natural_key', None), 'mdisk_name':item.get('name', None), 'mdisk_status':item.get('status', None), 'available_capacity_bytes':item.get('available_capacity_bytes', None), 'capacity_bytes':item.get('capacity_bytes', None), 'class':item.get('class', None), 'drive_compression_ratio':item.get('drive_compression_ratio', None), 'mode':item.get('mode', None), 'pool':item.get('pool', None), 'total_compression_ratio':item.get('total_compression_ratio', None), 'volumes_count':item.get('volumes_count', None)}
                            self.report_metric("si.tenant.block.system.mdisks", item.get('capacity_bytes', 0), dimensions)
                    except requests.exceptions.RequestException as e:
                          print(f"An error occurred while calling internal component API: {e}")
                     
                for future in as_completed(vol_read_data_rate_stat_futures):
                    response_data = future.result()
                    for item in response_data or []:
                        dimensions={'vol_name': item.get('name', None), 'system_id': item['system_id']}
                        for metric in item['metrics']:
                           metric_timestamp = metric['time_stamp']
                           ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                           self.logger.info(f"sending metric vol read rate : {metric['volume_read_data_rate']} at : {ts} for system : {item['system_id']} for volume : {item['name']}")
                           self.report_metric("si.tenant.block.system.volumes.read.data.rate.utilization", metric['volume_read_data_rate'], dimensions, timestamp=ts)
                           
                for future in as_completed(vol_write_data_rate_stat_futures):
                    response_data = future.result()
                    for item in response_data or []:
                        dimensions={'vol_name': item.get('name', None), 'system_id': item['system_id']}
                        for metric in item['metrics']:
                           metric_timestamp = metric['time_stamp']
                           ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                           self.report_metric("si.tenant.block.system.volumes.write.data.rate.utilization", metric['volume_write_data_rate'], dimensions, timestamp=ts)

                for future in as_completed(vol_total_data_rate_stat_futures):
                    response_data = future.result()
                    for item in response_data or []:
                        dimensions={'vol_name': item.get('name', None), 'system_id': item['system_id']}
                        for metric in item['metrics']:
                           metric_timestamp = metric['time_stamp']
                           ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                           self.report_metric("si.tenant.block.system.volumes.total.data.rate.utilization", metric['volume_total_data_rate'], dimensions, timestamp=ts)
                     

                for future in as_completed(vol_overall_tot_io_rate_stat_futures):
                    response_data = future.result()
                    for item in response_data or []:
                        dimensions={'vol_name': item.get('name', None), 'system_id': item['system_id']}
                        for metric in item['metrics']:
                           metric_timestamp = metric['time_stamp']
                           ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                           self.report_metric("si.tenant.block.system.volumes.overall.tot.io.rate.utilization", metric['volume_overall_total_io_rate'], dimensions, timestamp=ts)

                for future in as_completed(vol_tot_resp_time_stat_futures):
                    response_data = future.result()
                    for item in response_data or []:
                        dimensions={'vol_name': item.get('name', None), 'system_id': item['system_id']}
                        for metric in item['metrics']:
                           metric_timestamp = metric['time_stamp']
                           ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                           self.report_metric("si.tenant.block.system.volumes.tot.response.time.rate.utilization", metric['volume_total_response_time'], dimensions, timestamp=ts)

                for future in as_completed(vol_overall_read_cache_hit_stat_futures):
                    response_data = future.result()
                    for item in response_data or []:
                        dimensions={'vol_name': item.get('name', None), 'system_id': item['system_id']}
                        for metric in item['metrics']:
                           metric_timestamp = metric['time_stamp']
                           ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                           self.report_metric("si.tenant.block.system.volumes.overall.read.cache.hit.per.utilization", metric['volume_overall_read_cache_hit_percentage'], dimensions, timestamp=ts)

                for future in as_completed(disk_tot_io_rate_stat_futures):
                    response_data = future.result()
                    for item in response_data or []:
                        dimensions={'disk_name': item.get('name', None), 'system_id': item['system_id']}
                        for metric in item['metrics']:
                           metric_timestamp = metric['time_stamp']
                           ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                           self.report_metric("si.tenant.block.system.drives.total.io.rate.utilization", metric['total_disk_io_rate'], dimensions, timestamp=ts)

                for future in as_completed(disk_total_data_rate_stat_futures):
                    response_data = future.result()
                    for item in response_data or []:
                        dimensions={'disk_name': item.get('name', None),'system_id': item['system_id']}
                        for metric in item['metrics']:
                           metric_timestamp = metric['time_stamp']
                           ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                           self.report_metric("si.tenant.block.system.drives.total.data.rate.utilization", metric['disk_total_data_rate'], dimensions, timestamp=ts)

                for future in as_completed(disk_total_resp_time_stat_futures):
                    response_data = future.result()
                    for item in response_data or []:
                         dimensions={'disk_name': item.get('name', None), 'system_id': item['system_id']}
                         for metric in item['metrics']:
                           metric_timestamp = metric['time_stamp']
                           ts= datetime.datetime.fromtimestamp(metric_timestamp/1000)
                           self.report_metric("si.tenant.block.system.drives.total.resp.time.utilization", metric['disk_total_response_time'], dimensions, timestamp=ts) """
         
                for item in storage_sytem:
                       dimensions={'block_system_name': item['name'], 'system_id':item['storage_system_id']}
                       self.report_metric("si.tenant.block.systems", item.get('capacity_bytes', 0), dimensions)
                print("Exit fetch_unified_analysis_screen_metrics method")

if __name__ == '__main__':
    main()
