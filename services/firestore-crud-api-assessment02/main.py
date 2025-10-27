# Import FastAPI framework and HTTPException for error handling
from fastapi import FastAPI, HTTPException

# Import BaseModel from Pydantic for data validation
from pydantic import BaseModel

# Import Optional type hint for optional fields
from typing import Optional

# Import Firestore client from Google Cloud
from google.cloud import firestore

# Import logging module for logging information and errors
import logging

# Create a FastAPI app instance
app = FastAPI()

# Initialize Firestore client
db = firestore.Client()

# Name of the Firestore collection to use
collection_name = "items"

# Configure logging format and level
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Define the data model for an item (used for creation)
class Item(BaseModel):
    name: str                           # Item name (required)
    description: Optional[str] = None   # Item description (optional)
    price: float                        # Item price (required)

# Define the data model for updating an item (all fields optional)
class ItemUpdate(BaseModel):
    name: Optional[str] = None          # Item name (optional)
    description: Optional[str] = None   # Item description (optional)
    price: Optional[float] = None       # Item price (optional)

# Define the data model for updating only the price
class PriceUpdate(BaseModel):
    price: float                        # New price (required)

# Endpoint to create a new item
@app.post("/items")
async def create_item(item: Item):
    try:
        logging.info("Creating item: %s", item.dict())  # Log the item data
        doc_ref, _ = db.collection(collection_name).add(item.dict())  # Add item to Firestore
        return {"id": doc_ref.id}                       # Return the new document ID
    except Exception as e:
        logging.exception("Error creating item: %s", str(e))  # Log the exception
        raise HTTPException(status_code=500, detail="Failed to create item")  # Return HTTP 500

# Endpoint to get an item by its ID
@app.get("/items/{item_id}")
async def get_item(item_id: str):
    doc = db.collection(collection_name).document(item_id).get()  # Get document from Firestore
    if doc.exists:
        return {"id": doc.id, **doc.to_dict()}                    # Return item data if found
    raise HTTPException(status_code=404, detail="Item not found") # Return HTTP 404 if not found

# Endpoint to update an item by its ID
@app.put("/items/{item_id}")
async def update_item(item_id: str, item: ItemUpdate):
    doc_ref = db.collection(collection_name).document(item_id)    # Reference to the document
    doc = doc_ref.get()                                           # Get the document
    if not doc.exists:
        raise HTTPException(status_code=404, detail="Item not found")  # Return 404 if not found

    updates = {k: v for k, v in item.dict().items() if v is not None}  # Prepare updates (ignore None)
    doc_ref.update(updates)                                       # Update the document in Firestore
    logging.info("Updated item %s with %s", item_id, updates)     # Log the update
    return {"message": "Item updated"}                            # Return success message

# Endpoint to delete an item by its ID
@app.delete("/items/{item_id}")
async def delete_item(item_id: str):
    db.collection(collection_name).document(item_id).delete()     # Delete the document from Firestore
    logging.info("Deleted item: %s", item_id)                     # Log the deletion
    return {"message": "Item deleted"}                            # Return success message

# Endpoint to list items with pagination support
@app.get("/items")
async def list_items(limit: int = 10, start_after: Optional[str] = None):
    query = db.collection(collection_name).order_by("name").limit(limit)  # Query items ordered by name
    if start_after:
        query = query.start_after({"name": start_after})          # Start after a specific item if provided
    docs = query.stream()                                         # Stream the query results
    return [{"id": doc.id, **doc.to_dict()} for doc in docs]      # Return list of items

# Endpoint to update only the price of an item using a transaction
@app.post("/items/{item_id}/update_price")
async def update_price(item_id: str, update: PriceUpdate):
    new_price = update.price                                      # Extract new price from request

    transaction = db.transaction()                                # Start a Firestore transaction
    doc_ref = db.collection(collection_name).document(item_id)    # Reference to the document

    # Define a transactional function to update the price
    @firestore.transactional
    def txn_fn(transaction, doc_ref, new_price):
        snap = doc_ref.get(transaction=transaction)               # Get document snapshot in transaction
        if not snap.exists:
            raise HTTPException(status_code=404, detail="Item not found")  # Return 404 if not found
        transaction.update(doc_ref, {"price": new_price})         # Update the price in transaction

    try:
        txn_fn(transaction, doc_ref, new_price)                   # Execute the transaction
        logging.info("Transaction: Updated price of item %s to %s", item_id, new_price)  # Log update
        return {"message": "Price updated", "new_price": new_price}  # Return success message
    except Exception as e:
        logging.exception("Error updating price")                 # Log any exception
        raise HTTPException(status_code=500, detail="Transaction failed")  # Return HTTP 500