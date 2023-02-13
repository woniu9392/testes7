package com;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEvent;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @Description 应用上下文对象获取holder
 * @Date 2019/12/10 9:47
 */
@Component
public class ApplicationContextHolder implements ApplicationContextAware, DisposableBean {

	/**
	 * 上下文对象实例
	 */
	private static ApplicationContext applicationContext;

	@SuppressWarnings("static-access")
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public static <T> T getBean(Class<T> t) {
		return applicationContext.getBean(t);
	}

	public static <T> Map<String, T> getBeansOfType(Class<T> t) {
		return applicationContext.getBeansOfType(t);
	}

	public static Object getBean(String name) {
		return applicationContext.getBean(name);
	}

	public static boolean containsBean(String name) {
		return applicationContext.containsBean(name);
	}

	public static boolean isSingleton(String name) {
		return applicationContext.isSingleton(name);
	}

	public static void publishEvent(ApplicationEvent event) {
		if (applicationContext != null) {
			applicationContext.publishEvent(event);
		}
	}

	@Override
	public void destroy() throws Exception {
		applicationContext = null;
	}

	/**
	 * 获取ApplicationName
	 */
	public static String getApplicationName() {
		return applicationContext.getApplicationName();
	}
}
