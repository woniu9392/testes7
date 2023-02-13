package com;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@EnableScheduling
public abstract class ElasticSearchCreateSchedule<T> {
	
	private Class<T> clazz;
	
	@Autowired
	private ElasticSearchConfig elasticSearchConfig;
	
	@Autowired
	private ElasticSearchService elasticSearchService;
	
	/**
	 * 任务休眠时间
	 */
	private final long scheduleSleepMillisecond = 500;
	
	@PostConstruct
	public void init() {
		RestHighLevelClient client = null;
		try {
			client = elasticSearchConfig.getClient();
			GetRequest getRequest = new GetRequest(getIndexName());
			boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
			if(!exists) {
				CreateIndexRequest request = new CreateIndexRequest(getIndexName());//建立索引
		        //建立的每一个索引均可以有与之关联的特定设置。
		        request.settings(Settings.builder()
		                .put("index.number_of_shards", elasticSearchConfig.getShards())
		                .put("index.number_of_replicas", elasticSearchConfig.getReplicas())
		        );
		        Map<String, Object> jsonMap = new HashMap<>();
	            Map<String, Object> mapping = new HashMap<>();
	            mapping.put("properties", getSourceMap());
	            jsonMap.put(getIndexName(), mapping);
		        //建立索引时建立文档类型映射
		        request.mapping(jsonMap);

		        //可选参数
		        request.setTimeout(TimeValue.timeValueMinutes(2));//超时,等待全部节点被确认(使用TimeValue方式)
		        request.setMasterTimeout(TimeValue.timeValueMinutes(1));//链接master节点的超时时间(使用TimeValue方式)
		        request.waitForActiveShards(ActiveShardCount.from(elasticSearchConfig.getReplicas()));//在建立索引API返回响应以前等待的活动分片副本的数量，以int形式表示。
		        ActionListener<CreateIndexResponse> listener = new ActionListener<CreateIndexResponse>() {
		            @Override
		            public void onResponse(CreateIndexResponse createIndexResponse) {
		                //若是执行成功，则调用onResponse方法;
		                log.info(String.format("create %s mapping %s", createIndexResponse.index(), createIndexResponse.isAcknowledged()));
		            }

		            @Override
		            public void onFailure(Exception e) {
		                //若是失败，则调用onFailure方法。
		                log.error(e.getMessage());
		            }
		        };
		        client.indices().createAsync(request, RequestOptions.DEFAULT, listener);//要执行的CreateIndexRequest和执行完成时要使用的ActionListener
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(client != null) {
					client.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private Map<String, Object> getSourceMap() {
		Map<String, Object> map = new HashMap<String, Object>();
		Field[] fields = clazz.getFields();
		String name;
		String type;
		for(Field field : fields) {
			name = field.getName();
			type = field.getGenericType().toString();
			map.put(name, ESDataTypeEnum.getMapByType(type));
		}
		return map;
	}
	
	@Scheduled(fixedDelay = scheduleSleepMillisecond)
	public void execute() {
		
	}
	
	/**
	 * 统一查询接口
	 * @param <T>
	 * @param querys 查询条件集合
	 * @param sort 排序
	 * @param pageNo 页码
	 * @param pageSize 每页条数
	 * @return 查询结果
	 */
	public Page<T> query(List<QueryCondition> querys, SortCondition sort, int pageNo, int pageSize){
		return elasticSearchService.query(getIndexName(), querys, sort, pageNo, pageSize);
	}
	
	/**
	 * 创建/修改索引
	 * @param list 数据集合
	 */
	public void index(List<T> list) {
		elasticSearchService.index(getIndexName(), list);
	}
	
	/**
	 * 删除索引
	 * @param ids 主键集合
	 */
	public void delete(List<String> ids) {
		elasticSearchService.delete(getIndexName(), ids);
	}
	
	protected abstract String getIndexName();
	
	protected abstract List<T> getData();
}
