package com.kappaware.jdchtable.config;


public class JdcConfigurationImpl implements JdcConfiguration {
	Parameters parameters;
	
	public JdcConfigurationImpl(Parameters parameters)  {
		this.parameters = parameters;
		
	}

	@Override
	public String getInputFile() {
		return parameters.getInputFile();
	}

	@Override
	public String getZookeeper() {
		return parameters.getZookeeper();
	}

	@Override
	public String getZnodeParent() {
		return parameters.getZnodeParent();
	}
	
	
	
		
}
