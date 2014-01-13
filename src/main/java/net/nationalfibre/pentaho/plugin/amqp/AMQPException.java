package net.nationalfibre.pentaho.plugin.amqp;

public class AMQPException extends RuntimeException
{
    private static final long serialVersionUID = 392771364876785298L;

    public AMQPException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public AMQPException(String message)
    {
        super(message);
    }
}
