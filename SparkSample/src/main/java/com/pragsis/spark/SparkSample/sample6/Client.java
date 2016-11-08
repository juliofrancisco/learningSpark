package com.pragsis.spark.SparkSample.sample6;

import java.io.Serializable;

public class Client implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private int idCliente;
	private String residencia;
	private String codigoOperacion;
	
	public int getIdCliente() {
		return idCliente;
	}
	public void setIdCliente(int idCliente) {
		this.idCliente = idCliente;
	}
	public String getResidencia() {
		return residencia;
	}
	public void setResidencia(String residencia) {
		this.residencia = residencia;
	}
	public String getCodigoOperacion() {
		return codigoOperacion;
	}
	public void setCodigoOperacion(String codigoOperacion) {
		this.codigoOperacion = codigoOperacion;
	}
	@Override
	public String toString() {
		return "Client [idCliente=" + idCliente + ", residencia=" + residencia + ", codigoOperacion=" + codigoOperacion
				+ "]";
	}	
	
}
