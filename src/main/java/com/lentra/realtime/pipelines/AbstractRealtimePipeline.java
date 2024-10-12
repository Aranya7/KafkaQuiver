package com.lentra.realtime.pipelines;

import com.lentra.realtime.config.store.AppStore;
import com.lentra.realtime.constants.PipelineConstants;
import com.lentra.realtime.entity.endpoint.DestinationEndPoint;
import com.lentra.realtime.entity.endpoint.EndPoint;
import com.lentra.realtime.entity.endpoint.SourceEndPoint;
import com.lentra.realtime.enums.OffsetPositions;
import com.lentra.realtime.exception.PipelineInitializationException;
import com.lentra.realtime.pipelines.impl.PipelineImplementation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractRealtimePipeline implements Pipeline {
	private static Logger logger = LogManager.getLogger(AbstractRealtimePipeline.class);

	private EndPoint sourceEndPoint;
	private DestinationEndPoint destinationEndPoint;

	protected String institutionId;
	protected String pipelineAppName;
	protected OffsetPositions readingOffsetPosition;
	protected PipelineImplementation implementation;

	protected AbstractRealtimePipeline(PipelineImplementation implementation) {
		this.implementation = implementation;
	}

	public String getPipelineAppName() {
		return pipelineAppName;
	}

	public void setPipelineAppName(String pipelineAppName) {
		this.pipelineAppName = pipelineAppName;
	}

	public OffsetPositions getReadingOffsetPosition() {
		return readingOffsetPosition;
	}

	public void setReadingOffsetPosition(OffsetPositions readingOffsetPosition) {
		this.readingOffsetPosition = readingOffsetPosition;
	}

	public String getInstitutionId() {
		return institutionId;
	}

	public void setInstitutionId(String institutionId) {
		this.institutionId = institutionId;
	}

	public EndPoint getSourceEndPoint() {
		return sourceEndPoint;
	}

	public void setSourceEndPoint(EndPoint sourceEndPoint) {
		this.sourceEndPoint = sourceEndPoint;
	}

	public DestinationEndPoint getDestinationEndPoint() {
		return destinationEndPoint;
	}

	public void setDestinationEndPoint(DestinationEndPoint destinationEndPoint) {
		this.destinationEndPoint = destinationEndPoint;
	}

	public PipelineImplementation getImplementation() {
		return implementation;
	}

	public void setImplementation(PipelineImplementation implementation) {
		this.implementation = implementation;
	}
	
	@Override
	public void initialize() throws PipelineInitializationException {
		initializePipeline();
		initializeSource();
		initializeDestination();
	}

	private void initializeAppName() throws PipelineInitializationException {
		pipelineAppName = AppStore.store.getPipelineConfiguration()
				.get(PipelineConstants.PIPELINE_APP_NAME_MAPPING_KEY);
		if (pipelineAppName == null || pipelineAppName.isEmpty()) {
			throw new PipelineInitializationException(PipelineConstants.PIPELINE_APP_NAME_MAPPING_KEY);
		}
		logger.debug("App Name : {}", pipelineAppName);
	}

	private void initializeOffsetPosition() throws PipelineInitializationException {
		try {
			readingOffsetPosition = OffsetPositions.valueOf(
					AppStore.store.getPipelineConfiguration().get(PipelineConstants.PIPELINE_CONSUMER_START_POINT_KEY));
		} catch (IllegalArgumentException | NullPointerException e) {
			throw new PipelineInitializationException(PipelineConstants.PIPELINE_CONSUMER_START_POINT_KEY);
		}
		logger.debug("Offset position : {}", readingOffsetPosition);
	}
	
	private void initializeInstitution()
	{
		institutionId = AppStore.store.getPipelineConfiguration()
				.get(PipelineConstants.PIPELINE_INSITUTION_MAPPING_KEY);
		logger.debug("Institution Id : {}", institutionId);
	}

	private void initializePipeline() throws PipelineInitializationException {
		initializeInstitution();
		initializeAppName();
		initializeOffsetPosition();
	}

	@Override
	public void initializeSource() throws PipelineInitializationException {
		
		logger.info("Starting the process to initialize the source endpoint.");
		sourceEndPoint = new SourceEndPoint();
		sourceEndPoint.initializeEndPoint();

		logger.debug("Source End Point configurations: \n {}" , sourceEndPoint);
	}

	@Override
	public void initializeDestination() throws PipelineInitializationException {
		logger.info("Starting the process to initialize the destination endpoint.");
		destinationEndPoint = new DestinationEndPoint();
		destinationEndPoint.initializeEndPoint();

		logger.debug("Destination End Point configurations: \n {}" , destinationEndPoint);
	}

}
