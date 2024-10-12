package com.lentra.realtime.exception;

public class NoPropertryReaderFoundError extends RuntimeException
{
	private static final long serialVersionUID = 1L;

	public NoPropertryReaderFoundError(String exceptionMessage)
	{
		super(exceptionMessage);
	}
}
