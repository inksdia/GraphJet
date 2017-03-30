package com.graph.rest;

import com.google.gson.Gson;
import com.graph.Impl.GraphJetServiceImpl;
import com.graph.Utils.Preconditions;
import com.graph.Helper;
import com.graph.service.GraphJetService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.FileNotFoundException;

/**
 * Created by saurav on 29/03/17.
 */
@Path("/graph")
@Component
public class GraphJetRestAPI {

    private final Logger logger = LoggerFactory.getLogger(GraphJetRestAPI.class);
    final Gson gson = new Gson();

    private GraphJetService graphJetService = new GraphJetServiceImpl();

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response welcome() {
        logger.debug("for testing REST API");
        return generateResponse("Welcome");
    }

    @POST
    @Path("/create")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createGraph() {
        logger.debug("Request to create new Graph");
        return generateResponse(graphJetService.createGraph());
    }

    @POST
    @Path("/insert")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response insertEdge(IngestMessageDTO ingestMessageDTO) throws FileNotFoundException {
        //For testing
        ingestMessageDTO = new IngestMessageDTO();
        ingestMessageDTO.setMessages(Helper.getMessages());
        logger.debug("Post call for Edge Insertion" + ingestMessageDTO);
        Preconditions.notNull(ingestMessageDTO, "EdgeDto can't be null");
        return generateResponse(graphJetService.insertEdge(ingestMessageDTO));
    }

    @GET
    @Path("/topUsers")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response topUsers(@QueryParam("count") int count) {
        return generateResponse(graphJetService.topUsers(count));
    }

    @GET
    @Path("/topMessagesByUserId/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response topMessage(@PathParam("userId") Long userId, @QueryParam("count") int count) {
        logger.debug("Request for top Messages");
        return generateResponse(graphJetService.topMessagesByUserId(userId, count));
    }

    @GET
    @Path("/topHashtags")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response topHashtags(@QueryParam("count") int count) {
        logger.debug("Request for top Hashtags");
        return generateResponse(graphJetService.topHashTags(count));
    }

    @GET
    @Path("/topUserMessages/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response topUserMessage(@PathParam("userId") Long userId, @QueryParam("count") int count) {
        logger.debug("Request for top " + count + " tweets by " + userId);
        return generateResponse(graphJetService.topMessagesByUserId(userId, count));
    }

    @GET
    @Path("/topMessageUsers/{msgId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response topTweetsUser(@PathParam("msgId") Long msgId, @QueryParam("count") int count) {
        logger.debug("Request for top " + count + " user related/connected to " + msgId + "tweets");
        return generateResponse(graphJetService.topUsersByMsgId(msgId, count));
    }

    @GET
    @Path("/similarHashTags/{hashTag}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response similarHashTags(@PathParam("hashTag") String hashTag, @QueryParam("count") int count) {
        logger.debug("Request for similar " + count + " hashtags for hashtag" + hashTag);
        Preconditions.notBlank(hashTag);
        return generateResponse(graphJetService.similarHashTags(hashTag, count));
    }

    @GET
    @Path("/topHashTagMessages/{hashTag}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response topHashTagMessages(@PathParam("hashTag") String hashTag, @QueryParam("count") int count) {
        logger.debug("Request for similar " + count + " hashtags for hashtag" + hashTag);
        Preconditions.notBlank(hashTag);
        Long hashTagId = Integer.toUnsignedLong(hashTag.hashCode());
        return generateResponse(graphJetService.topMessagesByHashTags(hashTagId, count));
    }

    @GET
    @Path("/topMessages")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response topTweets(@QueryParam("count") int count) {
        logger.debug("Request for top " + count + " users");
        return generateResponse(graphJetService.topMessages(count));
    }

    @GET
    @Path("/topInfluencers")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response topInfluencers(@QueryParam("count") int count) {
        logger.debug("Request for top " + count + " users");
        return generateResponse(graphJetService.topInfluencers(count));
    }


    private <T> Response generateResponse(T obj) {
        return Response.ok(gson.toJson(obj)).build();
    }

}
