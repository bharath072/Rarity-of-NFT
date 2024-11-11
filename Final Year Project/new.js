import Moralis from "moralis/node";
const serverUrl = "https://yoursubdomain.usemoralis.com:2053/server";
app_id = "EWqWGLCnqtkZlf8x27j2Ji5axSAUtqpZt2LgthG02ZSThkJSl9URAXFcyAybLLqY"
contract_address = "0x60E4d786628Fea6478F785A6d7e704777c86a7c6"

let owners: any

async function start(){
  await Moralis.start({ serverUrl: serverUrl, appId: appId });

  // Get first 500
  await getOwners();

  // Simulate old page size by dividing page by 5
  console.log(Math.floor(owners.page / 5))

  // Get next 500
  await getOwners()
  console.log(Math.floor(owners.page / 5))
}

async function includeNextNPages(previous: any, numPages: number){

  // Define results for the remaining pages 
  const result = previous.result || [];

  // Always keep the latest response around
  let response = previous;

  // Loop through the remaining pages
  for (let i = 0; i < numPages; i++) {
  
    // Get the next page
    response = await response.next(); //Best Practice: Always use next() to get the next page

    // Exit if we are on the last page already
    if (response.result.length == 0) break

    // Add the results to the previous results
    result.push(...response.result);
  }

  // Apply the results to the last page
  response.result = result

  // Return the response
  return response
}

async function getOwners() {

  owners = await Moralis.Web3API.token.getNFTOwners({
    address: contractAddress,
    chain: "eth", // Best Practice: Always specify chain
    limit: 100, // Best Practice: Always specify limit. (use the lowest limit you need for faster response)
    cursor: owners ? owners.cursor : null, // Optional
  }).then((response) => includeNextNPages(response, 4));
}

start()