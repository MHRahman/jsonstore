package jsonstore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.bson.BsonArray;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;

@RestController
public class DataController {
	MongoDatabase db;
	public DataController() {
		MongoClient mc= new MongoClient("localhost", 27017);
		db= mc.getDatabase("mydb");
	}
	
	@RequestMapping(method=RequestMethod.GET, value="/{collection}/{field}/{value}")
	@ResponseBody
	public Object getRecordsByStringFieldValue(@PathVariable String collection, @PathVariable String field, @PathVariable String value) {
		FindIterable<Document> find2 = db.getCollection(collection).find(new Document(field, value));
	    List<Document> docs= new ArrayList<Document>();
	    for (Document doc : find2) docs.add(doc);
	    if (docs.size() < 1) return Document.parse("{}");
	    if (docs.size() == 1) return docs.get(0);
	    else return docs;
	}

	@RequestMapping(method=RequestMethod.DELETE, value="/{collection}/{field}/{value}")
	@ResponseBody
	public DeleteResult deleteRecordsByStringFieldValue(@PathVariable String collection, @PathVariable String field, @PathVariable String value) {
		DeleteResult deleted = db.getCollection(collection).deleteMany(new Document(field, value));
	    return deleted;
	}
	
	@RequestMapping(method=RequestMethod.GET, value="/{collection}/$latest/{n}")
	@ResponseBody
	public Object getRecordsByStringFieldValue(@PathVariable String collection, @PathVariable Integer n) {
		FindIterable<Document> find2 = db.getCollection(collection).find(new Document()).sort(new Document("_id", -1)).limit(n);
	    List<Document> docs= new ArrayList<Document>();
	    for (Document doc : find2) docs.add(doc);
	    if (docs.size() < 1) return Document.parse("{}");
	    if (docs.size() == 1) return docs.get(0);
	    else return docs;
	}
	
	@RequestMapping(method=RequestMethod.POST, value="/{collection}")
	@ResponseBody
	public Document addRecordToCollection(@PathVariable String collection, @RequestBody String json) {
	    Document doc= Document.parse(json);
	    db.getCollection(collection).insertOne(doc);
	    return doc;
	}
	
	@RequestMapping(method=RequestMethod.PUT, value="/{collection}/{field}/{value}")
	@ResponseBody
	public Document updateRecordInCollection(@PathVariable String collection, @PathVariable String field, @PathVariable String value, @RequestBody String json) {
	    Document doc= Document.parse(json);
	    FindIterable<Document> find = db.getCollection(collection).find(new Document(field, value));
	    //TODO update document
	    return doc;
	}
}
