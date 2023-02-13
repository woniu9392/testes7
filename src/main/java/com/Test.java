package com;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.alibaba.fastjson.JSONObject;

@SpringBootApplication
public class Test {
	
//	public static String indexName = "collect4vat_test";
	public static String indexName = "index_job_issue_all";

	public static void main(String[] args) {
		SpringApplication.run(Test.class, args);
//		get("job_issue_defect:1554432903466180608");
//		search();
//		searchMock();
		showAllIndex();
//		drop();
//		index("00007eb5ef2443e1b8211b7c6b1d0751");
//		update("00007eb5ef2443e1b8211b7c6b1d0751");
//		get(Arrays.asList("00007eb5ef2443e1b8211b7c6b1d0751", "0001f9c42f0b4de0b85429315f938d2c"));
		System.exit(0);
	}
	
	/**
	 * 查看所有索引名称
	 */
	public static void showAllIndex() {
		ElasticSearchConfig config = ApplicationContextHolder.getBean(ElasticSearchConfig.class);
		RestHighLevelClient client = null;
		try {
			client = config.getClient();
			GetIndexRequest getIndexRequest = new GetIndexRequest("*");
			GetIndexResponse getIndexResponse = client.indices().get(getIndexRequest, RequestOptions.DEFAULT);
			String[] indices = getIndexResponse.getIndices();
			List<String> asList = Arrays.asList(indices);
			System.out.println(asList);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(client != null)
					client.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 查询实例
	 */
	public static void search() {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
//		QueryBuilder query = QueryBuilders.matchPhraseQuery("moduleIds", "122");
//		bqb.must(query);
//		QueryBuilder query1 = QueryBuilders.matchPhraseQuery("invoiceCode", "4200211130");
//		bqb.must(query1);
//		QueryBuilder query4 = QueryBuilders.matchPhraseQuery("invoiceNum", "08405952");
//		bqb.must(query4);
//		QueryBuilder query2 = QueryBuilders.wildcardQuery("issueId.keyword", "*,122,*");
//		bqb.must(query2);
		QueryBuilder query3 = QueryBuilders.matchPhraseQuery("tableName", "job_issue");
		bqb.must(query3);
//		RangeQueryBuilder range = QueryBuilders.rangeQuery("issueId");
//		range.gt("0");
//		RangeQueryBuilder range2 = QueryBuilders.rangeQuery("createdAt");
//		range2.gt("1651631731");
//		bqb.must(range2);
		SearchRequest searchRequest = new SearchRequest(indexName);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.from(0);
		searchSourceBuilder.size(50);
		searchSourceBuilder.query(bqb);
		searchSourceBuilder.sort("updatedAt", SortOrder.DESC);
		searchSourceBuilder.trackTotalHits(true);
//		searchSourceBuilder.trackScores(true);
		searchRequest.source(searchSourceBuilder);
		ElasticSearchConfig config = ApplicationContextHolder.getBean(ElasticSearchConfig.class);
		RestHighLevelClient client = null;
		try {
			client = config.getClient();
			SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
			SearchHits hits = searchResponse.getHits();
			System.out.println("查询总数：" + hits.getTotalHits().value);
			SearchHit[] searchHits = hits.getHits();
			Map<String, Object> sourceAsMap;
			for (SearchHit searchHit : searchHits) {
				sourceAsMap = searchHit.getSourceAsMap();
				System.out.println("ID：" + searchHit.getId());
				System.out.println("数据详情：" + sourceAsMap);
//				for(Entry<String, Object> entry : sourceAsMap.entrySet()) {
//					System.out.print(entry.getKey() + ":" + entry.getValue().toString());
//				}
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
	
	/**
	 * 查询实例
	 */
	public static void searchMock() {
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		QueryBuilder query = QueryBuilders.matchPhraseQuery("projectId", "dam");
		bqb.must(query);
		BoolQueryBuilder bqb1 = QueryBuilders.boolQuery();
		QueryBuilder query1 = QueryBuilders.matchPhraseQuery("title", "测试筛选器");
		bqb1.should(query1);
		QueryBuilder query2 = QueryBuilders.matchPhraseQuery("projectCode", "测试筛选器");
		bqb1.should(query2);
		QueryBuilder query3 = QueryBuilders.matchPhraseQuery("description", "测试筛选器");
		bqb1.should(query3);
		bqb.must(bqb1);
		BoolQueryBuilder bqb2 = QueryBuilders.boolQuery();
		QueryBuilder query4 = QueryBuilders.wildcardQuery("moduleIds.keyword", "*,122,*");
		bqb2.should(query4);
		bqb.must(bqb2);
		SearchRequest searchRequest = new SearchRequest(indexName);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.from(0);
		searchSourceBuilder.size(10);
		searchSourceBuilder.query(bqb);
		searchSourceBuilder.sort("dueDate", SortOrder.DESC);
		searchSourceBuilder.trackTotalHits(true);
//		searchSourceBuilder.trackScores(true);
		searchRequest.source(searchSourceBuilder);
		ElasticSearchConfig config = ApplicationContextHolder.getBean(ElasticSearchConfig.class);
		RestHighLevelClient client = null;
		try {
			client = config.getClient();
			SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
			SearchHits hits = searchResponse.getHits();
			System.out.println("查询总数：" + hits.getTotalHits().value);
			SearchHit[] searchHits = hits.getHits();
			Map<String, Object> sourceAsMap;
			for (SearchHit searchHit : searchHits) {
				sourceAsMap = searchHit.getSourceAsMap();
				System.out.println("ID：" + searchHit.getId());
				System.out.println("数据详情：" + sourceAsMap);
//				for(Entry<String, Object> entry : sourceAsMap.entrySet()) {
//					System.out.print(entry.getKey() + ":" + entry.getValue().toString());
//				}
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
	
	public static void get(String id) {
		ElasticSearchConfig config = ApplicationContextHolder.getBean(ElasticSearchConfig.class);
		RestHighLevelClient client = null;
		try {
			client = config.getClient();
			GetRequest request = new GetRequest(indexName, id);
	        GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);
	        System.out.println(JSONObject.toJSON(getResponse));
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
	
	public static void get(List<String> ids) {
		ElasticSearchConfig config = ApplicationContextHolder.getBean(ElasticSearchConfig.class);
		RestHighLevelClient client = null;
		try {
			client = config.getClient();
			MultiGetRequest request = new MultiGetRequest();
			for(String id : ids) {
				request.add(new MultiGetRequest.Item(indexName, id));
			}
			MultiGetResponse response = client.mget(request, RequestOptions.DEFAULT);
			MultiGetItemResponse[] itemResponses = response.getResponses();
		    for (MultiGetItemResponse itemResponse : itemResponses) {
		        if (itemResponse.getFailure() != null) {
		            continue;
		        }
		        GetResponse getResponse = itemResponse.getResponse();
		        if (getResponse.isExists()) {
		            String sourceAsString = getResponse.getSourceAsString();
		            System.out.println("数据详情：" + sourceAsString);
		        }
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
	
//	public static void drop() {
//		ElasticSearchConfig config = ApplicationContextHolder.getBean(ElasticSearchConfig.class);
//		RestHighLevelClient client = null;
//		try {
//			client = config.getClient();
//			DeleteIndexRequest request = new DeleteIndexRequest(indexName);
//			AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
//			System.out.println(response.isAcknowledged());
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			try {
//				if(client != null) {
//					client.close();
//				}
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//	}
	
	public static void update(String id) {
		TestEntity e = new TestEntity();
		e.setId(id);
		e.setPicreateId(123l);
		e.setRequserName("leader");
		List<TestEntity> list = new ArrayList<TestEntity>();
		list.add(e);
		update(list);
	}
	
	public static void index(String id) {
		TestEntity e = new TestEntity();
		e.setId(id);
		e.setPicreateId(123l);
		e.setRequserName("leader");
		List<TestEntity> list = new ArrayList<TestEntity>();
		list.add(e);
		index(list);
	}
	
	public static void update(List<TestEntity> list) {
		BulkRequest request = new BulkRequest();
		UpdateRequest index = null;
		String json = null;
		JSONObject o;
		for(TestEntity t : list) {
			json = JSONObject.toJSONString(t);
			o = JSONObject.parseObject(json);
			String id = o.getString("id");
			o.remove("id");
			json = o.toJSONString();
			index = new UpdateRequest(indexName, id);
			index.upsert(new IndexRequest().index(indexName)
				     .id(id)
				     .source(json, XContentType.JSON));
			request.add(index);
		}
		bulk(request);
	}
	
	public static void index(List<TestEntity> list) {
		BulkRequest request = new BulkRequest();
		IndexRequest index = null;
		String json = null;
		JSONObject o;
		for(TestEntity t : list) {
			json = JSONObject.toJSONString(t);
			o = JSONObject.parseObject(json);
			String id = o.getString("id");
			o.remove("id");
			json = o.toJSONString();
			index = new IndexRequest();
			index.index(indexName)
			     .id(id)
			     .source(json, XContentType.JSON);
			request.add(index);
		}
		bulk(request);
	}
	
	private static void bulk(BulkRequest bulkRequest) {
		ElasticSearchConfig config = ApplicationContextHolder.getBean(ElasticSearchConfig.class);
		RestHighLevelClient client = null;
		try {
			client = config.getClient();
			BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
			response.forEach(item ->{
				if(item.isFailed()) {
					if(item.getOpType() == OpType.INDEX || item.getOpType() == OpType.CREATE) {
						System.out.println("索引名称：" + item.getIndex() + "，创建索引失败，数据id：:" + item.getId() + "，数据内容：" + JSONObject.toJSONString(item));
					}else if(item.getOpType() == OpType.DELETE) {
						System.out.println("索引名称：" + item.getIndex() + "，删除索引失败，数据id：:" + item.getId());
					}
					
				}
			});
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
}
