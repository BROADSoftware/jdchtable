package com.kappaware.jdchtable;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeanHelper {
	static Logger log = LoggerFactory.getLogger(BeanHelper.class);

	private Map<String, PropertyDescriptor> propertyDescriptorByPoperty = new HashMap<String, PropertyDescriptor>();
	private String entityName;
	private Class<?> beanClass;

	public BeanHelper(Class<?> beanClass, String entityName) {
		this.beanClass = beanClass;
		this.entityName = entityName;
		BeanInfo info;
		try {
			info = Introspector.getBeanInfo(this.beanClass);
		} catch (IntrospectionException e) {
			throw new RuntimeException(String.format("Unable to introspect class '%s'", beanClass.getCanonicalName()), e);
		}
		for (PropertyDescriptor pd : info.getPropertyDescriptors()) {
			log.debug(String.format("Class '%s': Find property '%s' of type '%s'", this.beanClass.getCanonicalName(), pd.getName(), pd.getPropertyType().getCanonicalName()));
			this.propertyDescriptorByPoperty.put(pd.getName(), pd);
		}
	}

	/**
	 * Set value of a property
	 * @param o
	 * @param propertyName
	 * @param propertyValue
	 * @return 1 if the property is modified. 0 if value was already set to the target value
	 * @throws DescriptionException
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	int set(Object o, String propertyName, Object propertyValue) throws DescriptionException {
		PropertyDescriptor pd = this.propertyDescriptorByPoperty.get(propertyName);
		if (pd == null) {
			throw new DescriptionException(String.format("Property '%s' is not valid for an HBase %s!", propertyName, entityName));
		}
		Class<?> clazz = pd.getPropertyType();
		Object value;
		if (clazz.isEnum()) {
			value = Utils.parseEnum((Class<Enum>) clazz, propertyValue, propertyName, null);
		} else if (clazz.isAssignableFrom(Long.class) || clazz.isAssignableFrom(long.class)) {
			if (propertyValue instanceof Number) {
				value = ((Number) propertyValue).longValue();
			} else {
				try {
					value = Long.parseLong(propertyValue.toString());
				} catch (Exception e) {
					throw new DescriptionException(String.format("Value '%s' is not valid for numeric property '%s' of an HBase %s!", propertyValue, propertyName, this.entityName));
				}
			}
		} else if (clazz.isAssignableFrom(Integer.class) || clazz.isAssignableFrom(int.class)) {
			if (propertyValue instanceof Number) {
				value = ((Number) propertyValue).intValue();
			} else {
				try {
					value = Integer.parseInt(propertyValue.toString());
				} catch (Exception e) {
					throw new DescriptionException(String.format("Value '%s' is not valid for numeric property '%s' of an HBase %s!", propertyValue, propertyName, this.entityName));
				}
			}
		} else if (clazz.isAssignableFrom(Boolean.class) || clazz.isAssignableFrom(boolean.class)) {
			if(propertyValue instanceof Boolean) {
				value = (Boolean)propertyValue;
			} else {
				value = Utils.parseBoolean(propertyValue.toString(), null, propertyName);
			}
		} else if (clazz.isAssignableFrom(String.class)) {
			value = propertyValue;
		} else {
			throw new DescriptionException(String.format("Unable to manage property '%s' on HBase %s. Type '%s' is not supported!", propertyName, this.entityName, clazz.getCanonicalName()));
		}

		try {
			Object originalValue = pd.getReadMethod().invoke(o);
			if (!value.equals(originalValue)) {
				this.getWriteMethodExt(pd).invoke(o, value);
				return 1;
			} else {
				return 0;
			}
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw new RuntimeException(String.format("Unable to access property %s on HBase %s.", propertyName, this.entityName), e);
		}
	}
	
	Method getWriteMethodExt(PropertyDescriptor pd) throws DescriptionException {
		if(pd.getWriteMethod() == null) {
			// As some setter does not return 'void', they are not perceived as real setter. Must target them explicitly
			String mn = pd.getDisplayName();
			mn = "set" + mn.substring(0,  1).toUpperCase() + mn.substring(1);
			try {
				return beanClass.getMethod(mn, new Class<?>[] { pd.getPropertyType() });
			} catch (NoSuchMethodException | SecurityException e) {
				throw new DescriptionException(String.format("Unable to find a write Method (%s) for property %s on HBase %s", mn, pd.getDisplayName(), this.entityName));
			}
		} else {
			return pd.getWriteMethod();
		}
	}

}
