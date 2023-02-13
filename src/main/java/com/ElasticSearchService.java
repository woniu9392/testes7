package com;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.fastjson.JSONObject;

@Service
public class ElasticSearchService {
	
	@Autowired
	private ElasticSearchConfig elasticSearchConfig;
	
	private String primaryKey = "id";

	/**
	 * 统一查询接口
	 * @param <T>
	 * @param indexName 索引名称
	 * @param querys 查询条件
	 * @param sort 排序
	 * @param pageNo 页码（起始也数为1）
	 * @param pageSize 每页条数
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <T> Page<T> query(String indexName, List<QueryCondition> querys, SortCondition sort, int pageNo, int pageSize){
		Page<T> page = new Page<T>();
		page.setPageNo(pageNo);
		page.setPageSize(pageSize);
		BoolQueryBuilder bqb = QueryBuilders.boolQuery();
		BoolQueryBuilder subBqb;
		for(QueryCondition q : querys) {
			subBqb = QueryBuilders.boolQuery();
			if(q.getQueryType() == SearchTypeEnum.TERM_QUERY.getType()) {
				for(String keyword : q.getConditions()) {
					if(q.isAndRelation())
						subBqb.must(QueryBuilders.termQuery(q.getFieldName(), keyword));
					else
						subBqb.should(QueryBuilders.termQuery(q.getFieldName(), keyword));
				}
			}else if(q.getQueryType() == SearchTypeEnum.PHRASE_QUERY.getType()) {
				for(String keyword : q.getConditions()) {
					if(q.isAndRelation())
						subBqb.must(QueryBuilders.matchPhraseQuery(q.getFieldName(), keyword));
					else
						subBqb.should(QueryBuilders.matchPhraseQuery(q.getFieldName(), keyword));
				}
			}else if(q.getQueryType() == SearchTypeEnum.RANGE_QUERY.getType()) {
				if(q.getRangeStartCondition() == null && q.getRangeEndCondition() == null)
					continue;
				RangeQueryBuilder range = QueryBuilders.rangeQuery(q.getFieldName());
				if(q.getRangeStartCondition() != null) {
					range.gte(q.getRangeStartCondition());
				}
				if(q.getRangeEndCondition() != null) {
					range.lte(q.getRangeEndCondition());
				}
				subBqb.must(range);
			}else if(q.getQueryType() == SearchTypeEnum.WILD_CARD_QUERY.getType()) {
				for(String keyword : q.getConditions()) {
					if(q.isAndRelation())
						subBqb.must(QueryBuilders.wildcardQuery(q.getFieldName(), keyword));
					else
						subBqb.should(QueryBuilders.wildcardQuery(q.getFieldName(), keyword));
				}
			}
			bqb.must(subBqb);
		}
		SearchRequest searchRequest = new SearchRequest(indexName);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.from(pageNo - 1);
		searchSourceBuilder.size(pageSize);
		searchSourceBuilder.query(bqb);
		searchRequest.source(searchSourceBuilder);
		RestHighLevelClient client = null;
		try {
			client = elasticSearchConfig.getClient();
			SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
			SearchHits hits = searchResponse.getHits();
			SearchHit[] searchHits = hits.getHits();
			if(page.getTotalCount() > 0) {
				List<T> list = new ArrayList<T>();
				page.setTotalCount(hits.getMaxScore());
				Class<?> clazz = page.getClazz();
				Field[] fields = clazz.getFields();
//				System.out.println(searchHits.length);
				JSONObject o;
				for (SearchHit searchHit : searchHits) {
					o = new JSONObject();
					for(Field field : fields) {
						o.put(field.getName(), searchHit.field(field.getName()).getValue().toString());
					}
//					System.out.println(o.toJSONString());
					list.add((T)JSONObject.parseObject(o.toJSONString(), clazz));
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
		return page;
	}
	
	/**
	 * 创建/修改索引
	 * @param <T>
	 * @param indexName 索引名称
	 * @param list 数据集合
	 */
	public <T> void index(String indexName, List<T> list) {
		BulkRequest request = new BulkRequest();
		IndexRequest index = null;
		String json = null;
		JSONObject o;
		for(T t : list) {
			json = JSONObject.toJSONString(t);
			o = JSONObject.parseObject(json);
			index = new IndexRequest();
			index.index(indexName)
			     .id(o.getString(primaryKey))
			     .source(json, XContentType.JSON);
			request.add(index);
		}
		bulk(request);
	}
	
	/**
	 * 删除索引
	 * @param indexName 索引名称
	 * @param ids 主键集合
	 */
	public void delete(String indexName, List<String> ids) {
		BulkRequest request = new BulkRequest();
		DeleteRequest delete = null;
		for(String id : ids) {
			delete = new DeleteRequest();
			delete.index(indexName).id(id);
			request.add(delete);
		}
		bulk(request);
	}
	
	private void bulk(BulkRequest bulkRequest) {
		RestHighLevelClient client = null;
		try {
			client = elasticSearchConfig.getClient();
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
