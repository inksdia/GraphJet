package com.spr.rest;

import com.spr.Utils.Preconditions;
import com.spr.service.GraphJetService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Created by saurav on 27/03/17.
 */
@Path("/graph")
@Component
public class GraphJetRestAPI {

    private final Logger logger = LoggerFactory.getLogger(GraphJetRestAPI.class);

    @Autowired
    private GraphJetService graphJetService;

    @POST
    @Path("/create")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createGraph() {
        logger.debug("Request to create new Graph");
        return Response.ok(graphJetService.createGraph()).build();
    }

    @POST
    @Path("/insert")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response insertEdge(InsertEdgeDTO insertEdgeDTO) {
        logger.debug("Post call for Edge Insertion" + insertEdgeDTO);
        Preconditions.notNull(insertEdgeDTO, "EdgeDto can't be null");
        return Response.ok(graphJetService.insertEdge(insertEdgeDTO)).build();
    }

    @GET
    @Path("/topUsers")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response topUsers(@QueryParam("count") int count) {
        return Response.ok(graphJetService.topUsers(count)).build();
    }

    @GET
    @Path("/topMessages")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response topMessage(@QueryParam("count") int count) {
        logger.debug("Request for top Messages");
        return Response.ok(graphJetService.topMessages(count)).build();
    }

    @GET
    @Path("/topHashtags")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response topHashtags(@QueryParam("count") int count) {
        logger.debug("Request for top Hashtags");
        return Response.ok(graphJetService.topHashTags(count)).build();
    }

    @GET
    @Path("/topUserMessages/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response topUserMessage(@PathParam("userId") Long userId, @QueryParam("count") int count) {
        logger.debug("Request for top " + count + " tweets by " + userId);
        return Response.ok(graphJetService.topMessages(userId, count)).build();
    }

    @GET
    @Path("/topMessageUsers/{msgId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response topTweetsUser(@PathParam("msgId") Long msgId, @QueryParam("count") int count) {
        logger.debug("Request for top " + count + " user related/connected to " + msgId + "tweets");
        return Response.ok(graphJetService.topUsers(msgId, count)).build();
    }

}
