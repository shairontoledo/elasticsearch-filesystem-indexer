package net.hashcode.fsindexer
import java.io.File
import java.util.Date
import net.sf.jmimemagic._

import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.codec.digest.DigestUtils
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.XContentFactory._
import scala.actors.Actor._

object Main {
  
  def fetchFiles(path:String)(op:File => Unit){
    for (file <- new File(path).listFiles if !file.isHidden){
      op(file)
      if (file.isDirectory){
        fetchFiles(file.getAbsolutePath)(op)        
      }
    } 
  }

  def main(args: Array[String]) = {
    val dir = new File(args(0))
    if (!dir.exists || dir.isFile || dir.isHidden) {
      printf("Directory not found %s\n",dir)
      System.exit(1)
    }
    
    val client = new TransportClient()
    client addTransportAddress(new InetSocketTransportAddress("0.0.0.0",9300))
    fetchFiles( dir.getAbsolutePath){
      file => {
        printf("Indexing %s\n",file)
        client.prepareIndex("files", "file", DigestUtils.md5Hex(file.getAbsolutePath))
        .setSource(document(file))
        .execute.actionGet
      }
    }
    client.close
  }
    
  def document(file:File) = {

    val json = jsonBuilder.startObject
    .field("name",file.getName)
    .field("parent",file.getParentFile.getAbsolutePath)
    .field("path",file.getAbsolutePath)
    .field("last_modified",new Date(file.lastModified))
    .field("size",file.length)
    .field("is_directory",file.isDirectory)
    
    if (file.isFile) {
      try{
        val m = Magic.getMagicMatch(file, true)
        json.field("description",m.getDescription)
        .field("extension",m.getExtension)
        .field("mimetype",m.getMimeType)
      }catch {
        case _ => json.field("description","unknown")
          .field("extension",file.getName.split("\\.").last.toLowerCase)
          .field("mimetype","application/octet-stream")
      }
    }
    json.endObject
  }
}